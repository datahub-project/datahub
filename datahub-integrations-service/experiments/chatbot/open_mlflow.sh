#!/bin/bash

# Convenience script to generate a presigned MLflow tracking server URL and open it in a browser

set -e

# Default values
TRACKING_SERVER_NAME="prod-mlflow-tracking-server-01"
REGION="us-west-2"
SESSION_DURATION="43200"
EXPIRES_IN="300"
PRINT_ONLY=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --tracking-server-name)
            TRACKING_SERVER_NAME="$2"
            shift 2
            ;;
        --profile)
            AWS_PROFILE="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --session-duration)
            SESSION_DURATION="$2"
            shift 2
            ;;
        --expires-in)
            EXPIRES_IN="$2"
            shift 2
            ;;
        --print-only)
            PRINT_ONLY=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Generate a presigned MLflow tracking server URL and open it in browser"
            echo ""
            echo "Options:"
            echo "  --tracking-server-name NAME    MLflow tracking server name (default: prod-mlflow-tracking-server-01)"
            echo "  --profile PROFILE              AWS profile to use (defaults to AWS_PROFILE env var)"
            echo "  --region REGION                AWS region (default: us-west-2)"
            echo "  --session-duration SECONDS     Session expiration duration (default: 1800)"
            echo "  --expires-in SECONDS           URL expiration in seconds (default: 300)"
            echo "  --print-only                   Print URL instead of opening in browser"
            echo "  -h, --help                     Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Build AWS command
AWS_CMD="aws sagemaker create-presigned-mlflow-tracking-server-url"
AWS_CMD="$AWS_CMD --tracking-server-name $TRACKING_SERVER_NAME"
AWS_CMD="$AWS_CMD --session-expiration-duration-in-seconds $SESSION_DURATION"
AWS_CMD="$AWS_CMD --expires-in-seconds $EXPIRES_IN"
AWS_CMD="$AWS_CMD --region $REGION"

# Add profile if specified or available in environment
if [[ -n "$AWS_PROFILE" ]]; then
    AWS_CMD="$AWS_CMD --profile $AWS_PROFILE"
    echo "Generating presigned URL for tracking server: $TRACKING_SERVER_NAME"
    echo "Using AWS profile: $AWS_PROFILE"
else
    echo "Generating presigned URL for tracking server: $TRACKING_SERVER_NAME"
    echo "Using default AWS profile"
fi

# Execute AWS command and extract URL
echo "Running: $AWS_CMD"
RESPONSE=$($AWS_CMD)

if [[ $? -ne 0 ]]; then
    echo "Error: Failed to generate presigned URL"
    exit 1
fi

# Extract URL from JSON response
URL=$(echo "$RESPONSE" | jq -r '.AuthorizedUrl')

if [[ "$URL" == "null" || -z "$URL" ]]; then
    echo "Error: Failed to extract URL from AWS response"
    echo "Response: $RESPONSE"
    exit 1
fi

echo "Generated URL: $URL"

if [[ "$PRINT_ONLY" == "true" ]]; then
    echo ""
    echo "URL copied above - paste into your browser"
else
    echo "Opening URL in browser..."
    open "$URL"
fi