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

echo "[1/5] Checking AWS credentials..."
aws sts get-caller-identity >/dev/null 2>&1 || { echo "ERROR: AWS CLI not configured. Run 'aws configure' first."; exit 1; }
echo "  OK"

echo "[2/5] Installing CDK CLI..."
npm list -g aws-cdk >/dev/null 2>&1 || npm install -g aws-cdk
echo "  OK"

echo "[3/5] Installing Python dependencies..."
cd "$(dirname "$0")/cdk"
pip install -r requirements.txt -q
echo "  OK"

echo "[4/5] Bootstrapping CDK (first time only)..."
cdk bootstrap 2>/dev/null || true
echo "  OK"

echo "[5/5] Deploying stack..."
if [ -n "$ALERT_EMAIL" ]; then
    cdk deploy --require-approval never --context alert_email="$ALERT_EMAIL"
else
    cdk deploy --require-approval never
fi

echo ""
echo "============================================"
echo "  Deploy complete!"
echo ""
echo "  NEXT STEP: Set Firebolt credentials:"
echo "  aws secretsmanager put-secret-value \\"
echo "    --secret-id firebolt/scheduler-credentials \\"
echo "    --secret-string '{\"client_id\":\"YOUR_ID\",\"client_secret\":\"YOUR_SECRET\"}'"
echo "============================================"
