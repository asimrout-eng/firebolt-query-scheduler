#!/bin/bash
set -e

ALERT_EMAIL="${1:-}"

echo "============================================"
echo "  Firebolt Query Scheduler - One-Click Deploy"
echo "============================================"
echo ""

# Check prerequisites
command -v node >/dev/null 2>&1 || { echo "ERROR: Node.js is required. Install from https://nodejs.org"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: Python 3 is required."; exit 1; }
command -v aws >/dev/null 2>&1 || { echo "ERROR: AWS CLI is required."; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "ERROR: Docker is required (for Lambda bundling)."; exit 1; }

echo "[1/7] Checking AWS credentials..."
aws sts get-caller-identity >/dev/null 2>&1 || { echo "ERROR: AWS CLI not configured. Run 'aws configure' first."; exit 1; }
echo "  OK"

echo "[2/7] Auto-detecting Firebolt credentials from CDC Lambda..."
SECRET_ARN=""
# Try common CDC Lambda function names
for FUNC_NAME in firebolt-cdc-handler firebolt-cdc-lambda firebolt_cdc_handler; do
    ARN=$(aws lambda get-function-configuration \
        --function-name "$FUNC_NAME" \
        --query "Environment.Variables.FIREBOLT_SECRET_ARN" \
        --output text 2>/dev/null || true)
    if [ -n "$ARN" ] && [ "$ARN" != "None" ] && [ "$ARN" != "null" ]; then
        SECRET_ARN="$ARN"
        echo "  Found existing secret from Lambda '$FUNC_NAME'"
        echo "  ARN: $SECRET_ARN"
        break
    fi
done

if [ -z "$SECRET_ARN" ]; then
    # Fallback: search Secrets Manager for any firebolt secret
    SECRET_ARN=$(aws secretsmanager list-secrets \
        --query "SecretList[?contains(Name, 'firebolt')].ARN | [0]" \
        --output text 2>/dev/null || true)
    if [ -n "$SECRET_ARN" ] && [ "$SECRET_ARN" != "None" ] && [ "$SECRET_ARN" != "null" ]; then
        SECRET_NAME=$(aws secretsmanager list-secrets \
            --query "SecretList[?contains(Name, 'firebolt')].Name | [0]" \
            --output text 2>/dev/null || true)
        echo "  Found existing secret in Secrets Manager: $SECRET_NAME"
        echo "  ARN: $SECRET_ARN"
    else
        SECRET_ARN=""
        echo "  No existing Firebolt secret found — will create a new one"
        echo "  (You'll need to set credentials after deploy)"
    fi
fi

echo "[3/7] Installing CDK CLI..."
npm list -g aws-cdk >/dev/null 2>&1 || npm install -g aws-cdk
echo "  OK"

echo "[4/7] Installing Python dependencies..."
cd "$(dirname "$0")/cdk"
pip install -r requirements.txt -q
echo "  OK"

echo "[5/7] Bootstrapping CDK (first time only)..."
cdk bootstrap 2>/dev/null || true
echo "  OK"

echo "[6/7] Uploading SQL files to s3://fcanalytics/firebolt_dms_job/scheduled_queries/..."
cd "$(dirname "$0")/.."
if ls queries/*.sql 1>/dev/null 2>&1; then
    aws s3 sync queries/ s3://fcanalytics/firebolt_dms_job/scheduled_queries/ --exclude "*" --include "*.sql"
    echo "  OK"
else
    echo "  No .sql files in queries/ — skipping upload"
fi

echo "[7/7] Deploying stack..."
cd cdk
CTX_ARGS="--context s3_bucket=fcanalytics --context s3_prefix=firebolt_dms_job/scheduled_queries"
if [ -n "$SECRET_ARN" ]; then
    CTX_ARGS="$CTX_ARGS --context firebolt_secret_arn=$SECRET_ARN"
fi
if [ -n "$ALERT_EMAIL" ]; then
    CTX_ARGS="$CTX_ARGS --context alert_email=$ALERT_EMAIL"
fi
cdk deploy --require-approval never $CTX_ARGS

echo ""
echo "============================================"
echo "  Deploy complete!"
echo ""
if [ -n "$SECRET_ARN" ]; then
    echo "  Firebolt credentials: Reusing existing secret"
    echo "  $SECRET_ARN"
else
    echo "  NEXT STEP: Set Firebolt credentials:"
    echo "  aws secretsmanager put-secret-value \\"
    echo "    --secret-id firebolt/scheduler-credentials \\"
    echo "    --secret-string '{\"client_id\":\"YOUR_ID\",\"client_secret\":\"YOUR_SECRET\"}'"
fi
echo ""
echo "  SQL files: s3://fcanalytics/firebolt_dms_job/scheduled_queries/"
echo "============================================"
