"""
CDK Stack: Firebolt Query Scheduler

Creates:
  - S3 bucket for SQL files
  - Lambda function (query executor + poller)
  - Step Functions state machine with:
      • Multi-statement loop (partial → re-invoke)
      • Submit-and-poll for long single-statement queries (Lambda timeout → poll engine)
      • Retry on failure (up to 2 retries = 3 total attempts)
  - EventBridge cron schedules (one per query)
  - SNS topic for email alerts
  - Secrets Manager for Firebolt credentials
  - IAM roles with least-privilege

Step Functions flow:

  Init ─► Execute ─► CheckResult
                        ├─ "completed" ─► NotifySuccess ─► Done
                        ├─ "partial"   ─► PrepareNext ─► Execute (loop)
                        └─ "failed"    ─► PrepareRetry ─► CheckRetryLimit
                                             ├─ retries left ─► WaitRetry ─► Execute
                                             └─ max reached  ─► NotifyFailure ─► Fail

  Execute (Lambda timeout caught) ─► WaitForEngine ─► Poll ─► CheckPoll
                                        ▲                       ├─ "running" ─► WaitForEngine (loop)
                                        │                       ├─ "done"    ─► NotifySuccess ─► Done
                                        │                       └─ otherwise ─► PrepareRetry ─► ...
                                        └───────────────────────┘
"""

import os
import json

import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct

MAX_RETRIES = 2  # 3 total attempts (1 original + 2 retries)
POLL_WAIT_SECONDS = 300  # 5 min between polls
RETRY_WAIT_SECONDS = 60  # 1 min between retries


class FireboltSchedulerStack(Stack):

    def __init__(self, scope: Construct, id: str, config: dict, **kwargs):
        super().__init__(scope, id, **kwargs)

        fb_config = config["firebolt"]
        schedules = config["schedules"]

        # ──────────────────────────────────────────────────────────
        # SNS Topic
        # ──────────────────────────────────────────────────────────
        alert_topic = sns.Topic(
            self, "AlertTopic",
            display_name="Firebolt Query Scheduler Alerts",
        )
        alert_email = self.node.try_get_context("alert_email")
        if alert_email:
            alert_topic.add_subscription(subs.EmailSubscription(alert_email))

        # ──────────────────────────────────────────────────────────
        # S3 Bucket — use existing client bucket or create new
        # ──────────────────────────────────────────────────────────
        existing_bucket = self.node.try_get_context("s3_bucket")
        if existing_bucket:
            sql_bucket = s3.Bucket.from_bucket_name(
                self, "SqlBucket", existing_bucket,
            )
        else:
            sql_bucket = s3.Bucket(
                self, "SqlBucket",
                bucket_name=f"firebolt-scheduler-{self.account}-{self.region}",
                removal_policy=RemovalPolicy.RETAIN,
                auto_delete_objects=False,
            )

        sql_prefix = self.node.try_get_context("s3_prefix") or "queries"

        queries_dir = os.path.join(os.path.dirname(__file__), "..", "queries")
        if os.path.isdir(queries_dir) and any(
            f.endswith(".sql") for f in os.listdir(queries_dir)
        ):
            s3deploy.BucketDeployment(
                self, "DeploySqlFiles",
                sources=[s3deploy.Source.asset(queries_dir)],
                destination_bucket=sql_bucket,
                destination_key_prefix=sql_prefix,
            )

        # ──────────────────────────────────────────────────────────
        # Secrets Manager
        # ──────────────────────────────────────────────────────────
        existing_secret_arn = self.node.try_get_context("firebolt_secret_arn")
        if existing_secret_arn:
            secret = secretsmanager.Secret.from_secret_complete_arn(
                self, "FireboltCredentials", existing_secret_arn,
            )
        else:
            secret = secretsmanager.Secret(
                self, "FireboltCredentials",
                secret_name="firebolt/scheduler-credentials",
                description="Firebolt service account credentials",
                generate_secret_string=secretsmanager.SecretStringGenerator(
                    secret_string_template=json.dumps({
                        "client_id": "REPLACE_ME",
                        "client_secret": "REPLACE_ME",
                    }),
                    generate_string_key="_placeholder",
                ),
            )

        # ──────────────────────────────────────────────────────────
        # Lambda Function
        # ──────────────────────────────────────────────────────────
        lambda_dir = os.path.join(os.path.dirname(__file__), "..", "lambda")

        executor_fn = _lambda.Function(
            self, "ExecutorFn",
            function_name="firebolt-query-executor",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=_lambda.Code.from_asset(
                lambda_dir,
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && "
                        "cp handler.py /asset-output/",
                    ],
                ),
            ),
            timeout=Duration.minutes(15),
            memory_size=512,
            environment={
                "FIREBOLT_SECRET_ARN": secret.secret_arn,
                "FIREBOLT_ACCOUNT": fb_config["account"],
                "FIREBOLT_ENGINE": fb_config["engine"],
                "FIREBOLT_DATABASE": fb_config["database"],
                "SQL_BUCKET": sql_bucket.bucket_name,
                "SNS_TOPIC_ARN": alert_topic.topic_arn,
            },
        )
        secret.grant_read(executor_fn)
        sql_bucket.grant_read(executor_fn)
        alert_topic.grant_publish(executor_fn)

        # ──────────────────────────────────────────────────────────
        # Step Functions — State Definitions
        # ──────────────────────────────────────────────────────────

        # --- Initialize: set starting state ---
        init_state = sfn.Pass(
            self, "Initialize",
            parameters={
                "query_name": sfn.JsonPath.string_at("$.query_name"),
                "s3_key": sfn.JsonPath.string_at("$.s3_key"),
                "engine": sfn.JsonPath.string_at("$.engine"),
                "next_index": 0,
                "retry_count": 0,
            },
        )

        # --- Execute statements (Lambda, 15 min) ---
        execute_task = sfn_tasks.LambdaInvoke(
            self, "ExecuteStatements",
            lambda_function=executor_fn,
            payload=sfn.TaskInput.from_object({
                "mode": "execute",
                "query_name": sfn.JsonPath.string_at("$.query_name"),
                "s3_key": sfn.JsonPath.string_at("$.s3_key"),
                "engine": sfn.JsonPath.string_at("$.engine"),
                "next_index": sfn.JsonPath.number_at("$.next_index"),
            }),
            result_selector={
                "status": sfn.JsonPath.string_at("$.Payload.status"),
                "query_name": sfn.JsonPath.string_at("$.Payload.query_name"),
                "next_index": sfn.JsonPath.number_at("$.Payload.next_index"),
                "total_statements": sfn.JsonPath.number_at("$.Payload.total_statements"),
                "statements_executed": sfn.JsonPath.number_at("$.Payload.statements_executed"),
                "duration_sec": sfn.JsonPath.number_at("$.Payload.duration_sec"),
                "error": sfn.JsonPath.string_at("$.Payload.error"),
            },
            result_path="$.exec_result",
        )

        # --- Check execution result ---
        check_result = sfn.Choice(self, "CheckResult")

        # --- Prepare next batch (for partial) ---
        prepare_next_batch = sfn.Pass(
            self, "PrepareNextBatch",
            parameters={
                "query_name": sfn.JsonPath.string_at("$.query_name"),
                "s3_key": sfn.JsonPath.string_at("$.s3_key"),
                "engine": sfn.JsonPath.string_at("$.engine"),
                "next_index": sfn.JsonPath.number_at("$.exec_result.next_index"),
                "retry_count": sfn.JsonPath.number_at("$.retry_count"),
            },
        )

        # --- Prepare retry (increment counter, reset index) ---
        prepare_retry = sfn.Pass(
            self, "PrepareRetry",
            parameters={
                "query_name": sfn.JsonPath.string_at("$.query_name"),
                "s3_key": sfn.JsonPath.string_at("$.s3_key"),
                "engine": sfn.JsonPath.string_at("$.engine"),
                "next_index": 0,
                "retry_count": sfn.JsonPath.number_at(
                    "States.MathAdd($.retry_count, 1)"
                ),
            },
        )

        # --- Check retry limit ---
        check_retry_limit = sfn.Choice(self, "CheckRetryLimit")

        # --- Wait before retry ---
        wait_before_retry = sfn.Wait(
            self, "WaitBeforeRetry",
            time=sfn.WaitTime.duration(Duration.seconds(RETRY_WAIT_SECONDS)),
        )

        # --- Wait for engine (poll interval) ---
        wait_for_engine = sfn.Wait(
            self, "WaitForEngine",
            time=sfn.WaitTime.duration(Duration.seconds(POLL_WAIT_SECONDS)),
        )

        # --- Poll running queries ---
        poll_task = sfn_tasks.LambdaInvoke(
            self, "PollRunningQueries",
            lambda_function=executor_fn,
            payload=sfn.TaskInput.from_object({
                "mode": "poll",
                "query_name": sfn.JsonPath.string_at("$.query_name"),
                "engine": sfn.JsonPath.string_at("$.engine"),
            }),
            result_selector={
                "status": sfn.JsonPath.string_at("$.Payload.status"),
                "active_queries": sfn.JsonPath.number_at("$.Payload.active_queries"),
                "error": sfn.JsonPath.string_at("$.Payload.error"),
            },
            result_path="$.poll_result",
        )
        poll_task.add_retry(
            errors=["States.ALL"],
            interval=Duration.seconds(30),
            max_attempts=3,
            backoff_rate=2,
        )

        # --- Check poll result ---
        check_poll = sfn.Choice(self, "CheckPoll")

        # --- Notifications ---
        notify_success = sfn_tasks.SnsPublish(
            self, "NotifySuccess",
            topic=alert_topic,
            message=sfn.TaskInput.from_object({
                "status": "SUCCESS",
                "query_name": sfn.JsonPath.string_at("$.query_name"),
            }),
            subject="[Firebolt] Query Completed",
            result_path=sfn.JsonPath.DISCARD,
        )

        notify_failure = sfn_tasks.SnsPublish(
            self, "NotifyFailure",
            topic=alert_topic,
            message=sfn.TaskInput.from_object({
                "status": "FAILED",
                "query_name": sfn.JsonPath.string_at("$.query_name"),
                "retry_count": sfn.JsonPath.number_at("$.retry_count"),
            }),
            subject="[Firebolt] Query FAILED (max retries exceeded)",
            result_path=sfn.JsonPath.DISCARD,
        )

        succeed_state = sfn.Succeed(self, "Done")
        fail_state = sfn.Fail(self, "Failed", cause="Max retries exceeded")

        # ──────────────────────────────────────────────────────────
        # Step Functions — Wire transitions
        # ──────────────────────────────────────────────────────────

        # Main chain: Init → Execute → CheckResult
        definition = init_state.next(execute_task).next(check_result)

        # CheckResult branches
        check_result.when(
            sfn.Condition.string_equals("$.exec_result.status", "completed"),
            notify_success.next(succeed_state),
        )
        check_result.when(
            sfn.Condition.string_equals("$.exec_result.status", "partial"),
            prepare_next_batch.next(execute_task),
        )
        check_result.when(
            sfn.Condition.string_equals("$.exec_result.status", "failed"),
            prepare_retry,
        )
        check_result.otherwise(prepare_retry)

        # Retry chain: PrepareRetry → CheckRetryLimit → retry or fail
        prepare_retry.next(check_retry_limit)
        check_retry_limit.when(
            sfn.Condition.number_less_than_equals("$.retry_count", MAX_RETRIES),
            wait_before_retry.next(execute_task),
        )
        check_retry_limit.otherwise(
            notify_failure.next(fail_state),
        )

        # Timeout chain: Execute catch → WaitForEngine → Poll → CheckPoll
        execute_task.add_catch(
            wait_for_engine,
            result_path="$.error_info",
        )
        wait_for_engine.next(poll_task).next(check_poll)

        check_poll.when(
            sfn.Condition.string_equals("$.poll_result.status", "running"),
            wait_for_engine,
        )
        check_poll.when(
            sfn.Condition.string_equals("$.poll_result.status", "done"),
            notify_success,
        )
        check_poll.otherwise(prepare_retry)

        # Poll errors (after built-in retries exhausted) → retry
        poll_task.add_catch(
            prepare_retry,
            result_path="$.poll_error",
        )

        # ──────────────────────────────────────────────────────────
        # Create state machine
        # ──────────────────────────────────────────────────────────
        state_machine = sfn.StateMachine(
            self, "QueryStateMachine",
            state_machine_name="firebolt-query-scheduler",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(2),
        )

        # ──────────────────────────────────────────────────────────
        # EventBridge Schedules
        # ──────────────────────────────────────────────────────────
        for idx, sched in enumerate(schedules):
            name = sched["name"]

            rule = events.Rule(
                self, f"Sched{idx:02d}_{name}",
                rule_name=f"firebolt-{name}",
                description=sched.get("description", f"Firebolt: {name}"),
                schedule=events.Schedule.expression(sched["schedule"]),
                enabled=True,
            )
            rule.add_target(
                events_targets.SfnStateMachine(
                    state_machine,
                    input=events.RuleTargetInput.from_object({
                        "query_name": name,
                        "s3_key": sched["s3_key"],
                        "engine": sched.get("engine", fb_config["engine"]),
                    }),
                )
            )

        # ──────────────────────────────────────────────────────────
        # Outputs
        # ──────────────────────────────────────────────────────────
        cdk.CfnOutput(self, "SqlBucketName", value=sql_bucket.bucket_name,
                       description="Upload .sql files to s3://BUCKET/queries/")
        cdk.CfnOutput(self, "StateMachineArn", value=state_machine.state_machine_arn)
        cdk.CfnOutput(self, "LambdaFunctionName", value=executor_fn.function_name)
        cdk.CfnOutput(self, "AlertTopicArn", value=alert_topic.topic_arn)
        cdk.CfnOutput(self, "SecretArn", value=secret.secret_arn,
                       description="Update with real Firebolt credentials after deploy")
