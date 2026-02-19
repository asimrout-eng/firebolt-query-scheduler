# Firebolt Query Scheduler

Schedules and runs SQL queries on Firebolt using AWS Step Functions + Lambda + EventBridge.

## Architecture

```
EventBridge (cron schedule)
    → Step Functions state machine
        → Lambda (executes SQL on Firebolt engine)
            → SNS email alert (success / failure)
```

### How each query type is handled

| Scenario | What happens |
|---|---|
| **Short query** (< 15 min, single statement) | Lambda runs it, returns "completed" |
| **Multi-statement query** (stages fit in 15 min each) | Lambda runs statements sequentially, returns "partial" when low on time. Step Functions re-invokes for remaining statements. |
| **Long single-statement query** (> 15 min) | Lambda submits query to Firebolt engine → Lambda times out → query keeps running on engine → Step Functions polls `information_schema.running_queries` every 5 min until done |
| **Any failure** | Step Functions retries the entire query up to 2 times (3 total attempts), then sends failure alert |

### Step Functions state machine diagram

```
Init ─► Execute ─► CheckResult
                     ├─ completed ─► NotifySuccess ─► Done ✓
                     ├─ partial   ─► PrepareNext ─► Execute (loop)
                     └─ failed    ─► PrepareRetry ─► CheckRetryLimit
                                       ├─ retries left ─► Wait 60s ─► Execute
                                       └─ max reached  ─► NotifyFailure ─► Fail ✗

Execute (Lambda timeout):
    ─► WaitForEngine (5 min) ─► Poll ─► CheckPoll
          ▲                              ├─ running ─┘ (loop)
          │                              ├─ done ─► NotifySuccess ─► Done ✓
                                         └─ error ─► PrepareRetry ─► ...
```

## Prerequisites

- AWS CLI configured (`aws configure`)
- Node.js 18+ (for CDK CLI)
- Python 3.11+
- Docker (for Lambda bundling during CDK deploy)
- AWS CDK CLI: `npm install -g aws-cdk`

## One-Click Deploy

### Step 1: Place SQL files

Put your `.sql` files in the `queries/` folder:

```
queries/
├── dm_collection_master.sql
├── dm_channel_master.sql
├── dm_tenure_master.sql
├── dm_pincode_master.sql
├── dm_disburse_master.sql
├── dm_lead_channel_details.sql
├── dm_lead_borrower_basic_details.sql
├── dm_lead_funnel_cd.sql
├── dm_loan_details.sql
├── dm_static_pool.sql
├── dm_loan_details_at_monthly_incremental.sql
├── dm_loan_details_at_monthly_optimized.sql
└── dm_lead_details.sql
```

### Step 2: Deploy

```bash
# Quick deploy script (handles everything)
./deploy.sh your-email@company.com

# OR manual CDK commands:
cd cdk
pip install -r requirements.txt
cdk bootstrap            # first time only
cdk deploy --context alert_email=your-email@company.com
```

**Reusing existing Firebolt secret** (from CDC Lambda):

```bash
cdk deploy \
  --context alert_email=your-email@company.com \
  --context firebolt_secret_arn=arn:aws:secretsmanager:ap-south-1:ACCOUNT:secret:firebolt/faircent-credentials-XXXXX
```

### Step 3: Set Firebolt credentials (skip if reusing existing secret)

```bash
aws secretsmanager put-secret-value \
  --secret-id firebolt/scheduler-credentials \
  --secret-string '{
    "client_id": "YOUR_FIREBOLT_CLIENT_ID",
    "client_secret": "YOUR_FIREBOLT_CLIENT_SECRET"
  }'
```

## What gets created

| Resource | Name | Purpose |
|---|---|---|
| S3 Bucket | `firebolt-scheduler-ACCOUNT-REGION` | Stores SQL files |
| Lambda | `firebolt-query-executor` | Runs queries + polls engine |
| Step Functions | `firebolt-query-scheduler` | Orchestrates execution, retry, polling |
| EventBridge Rules | `firebolt-dm_*` (13 rules) | Cron triggers |
| SNS Topic | Firebolt Query Scheduler Alerts | Email alerts |
| Secrets Manager | `firebolt/scheduler-credentials` | Firebolt credentials |

## Configuration

Edit `config.json` to manage schedules:

```json
{
  "firebolt": {
    "account": "faircentindia",
    "database": "fair",
    "engine": "dm_engine"
  },
  "schedules": [
    {
      "name": "dm_collection_master",
      "s3_key": "queries/dm_collection_master.sql",
      "schedule": "cron(0 2 * * ? *)",
      "description": "Daily at 2:00 AM UTC"
    }
  ]
}
```

### Cron format

`cron(minute hour day-of-month month day-of-week year)`

**Note:** EventBridge uses **UTC**. IST = UTC + 5:30.

| IST time | UTC expression |
|---|---|
| 2:00 AM IST | `cron(30 20 * * ? *)` (8:30 PM UTC prev day) |
| 7:30 AM IST | `cron(0 2 * * ? *)` |
| Monthly 1st 3 AM IST | `cron(30 21 L * ? *)` |

## Updating SQL Queries

No redeployment needed. Upload directly to S3:

```bash
aws s3 cp my_updated_query.sql s3://BUCKET_NAME/queries/my_query.sql
```

## Adding a New Schedule

1. Add SQL file to `queries/`
2. Add entry to `config.json`
3. Run `cd cdk && cdk deploy`

## Monitoring

**Step Functions console:** AWS Console → Step Functions → `firebolt-query-scheduler` → Executions

**Manual trigger (testing):**

```bash
aws stepfunctions start-execution \
  --state-machine-arn STATE_MACHINE_ARN \
  --input '{"query_name":"dm_collection_master","s3_key":"queries/dm_collection_master.sql","engine":"dm_engine"}'
```

## Retry Behavior

- On failure, Step Functions retries the full query up to **2 times** (3 total attempts)
- 60-second wait between retries
- Retries reset to statement 0 (full re-execution)
- After max retries: failure email sent, execution marked as Failed

## Tear Down

```bash
cd cdk && cdk destroy
```

The S3 bucket is retained (not deleted) for safety.
