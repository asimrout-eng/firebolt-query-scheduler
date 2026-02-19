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

echo "[1/6] Checking AWS credentials..."
aws sts get-caller-identity >/dev/null 2>&1 || { echo "ERROR: AWS CLI not configured. Run 'aws configure' first."; exit 1; }
echo "  OK"

echo "[2/6] Installing CDK CLI..."
npm list -g aws-cdk >/dev/null 2>&1 || npm install -g aws-cdk
echo "  OK"

echo "[3/6] Installing Python dependencies..."
cd "$(dirname "$0")/cdk"
pip install -r requirements.txt -q
echo "  OK"

echo "[4/6] Bootstrapping CDK (first time only)..."
cdk bootstrap 2>/dev/null || true
echo "  OK"

echo "[5/6] Uploading SQL files to s3://fcanalytics/firebolt_dms_job/scheduled_queries/..."
cd "$(dirname "$0")/.."
if ls queries/*.sql 1>/dev/null 2>&1; then
    aws s3 sync queries/ s3://fcanalytics/firebolt_dms_job/scheduled_queries/ --exclude "*" --include "*.sql"
    echo "  OK"
else
    echo "  No .sql files in queries/ — skipping upload"
fi

echo "[6/6] Deploying stack..."
cd cdk
CTX_ARGS="--context s3_bucket=fcanalytics --context s3_prefix=firebolt_dms_job/scheduled_queries"
if [ -n "$ALERT_EMAIL" ]; then
    CTX_ARGS="$CTX_ARGS --context alert_email=$ALERT_EMAIL"
fi
cdk deploy --require-approval never $CTX_ARGS

echo ""
echo "============================================"
echo "  Deploy complete!"
echo ""
echo "  NEXT STEP: Set Firebolt credentials:"
echo "  aws secretsmanager put-secret-value \\"
echo "    --secret-id firebolt/scheduler-credentials \\"
echo "    --secret-string '{\"client_id\":\"YOUR_ID\",\"client_secret\":\"YOUR_SECRET\"}'"
echo ""
echo "  SQL files location:"
echo "  s3://fcanalytics/firebolt_dms_job/scheduled_queries/"
echo "============================================"
