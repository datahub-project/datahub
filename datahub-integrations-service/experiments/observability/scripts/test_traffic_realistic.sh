#!/bin/bash
# Generate realistic traffic using AWS Bedrock and DataHub context

set -e
cd "$(dirname "$0")/../../.."

echo "🤖 Realistic Traffic Generator (AWS Bedrock)"
echo ""
echo "This will:"
echo "  1. Sketch your DataHub backend (entity counts, platforms, top assets)"
echo "  2. Use AWS Bedrock (Claude) to generate contextual questions"
echo "  3. Maintain conversations with follow-up questions"
echo "  4. Start new conversations after 10 messages"
echo ""

# Check for AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo "❌ Error: AWS credentials not configured"
    echo ""
    echo "Please run: aws sso login --profile your-profile"
    echo "Or set AWS_PROFILE environment variable"
    exit 1
fi

echo "✓ AWS credentials found"
echo ""

# Default to 3 conversations, 10 messages each
CONVERSATIONS="${1:-3}"

echo "Generating $CONVERSATIONS realistic conversations..."
echo ""

source venv/bin/activate
python experiments/observability/chat_simulator.py \
    --conversations "$CONVERSATIONS" \
    --max-length 10 \
    --delay 3
