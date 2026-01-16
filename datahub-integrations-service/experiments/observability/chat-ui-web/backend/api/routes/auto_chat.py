"""
Auto-Chat Routes - API endpoints for automated chat generation.

Provides stateless question generation endpoint.
Frontend drives the auto-chat loop.
"""

from fastapi import APIRouter, Depends, HTTPException
from loguru import logger
from pydantic import BaseModel, Field

from core.auto_chat import get_question_generator
from core.chat_engine import ChatEngine

from ..dependencies import get_chat_engine

router = APIRouter(prefix="/api/auto-chat", tags=["auto-chat"])


class GenerateQuestionRequest(BaseModel):
    """Request to generate a question."""

    aws_profile: str | None = Field(default=None, description="AWS profile name for Bedrock")


class GenerateQuestionResponse(BaseModel):
    """Response with generated question."""

    success: bool
    question: str | None = None
    error: str | None = None


@router.post("/generate-question", response_model=GenerateQuestionResponse)
async def generate_question(
    request: GenerateQuestionRequest,
    engine: ChatEngine = Depends(get_chat_engine),
):
    """
    Generate a contextual question about DataHub.

    Args:
        request: Generate question request
        engine: Chat engine (injected)

    Returns:
        Generated question
    """
    try:
        generator = get_question_generator()
        question = generator.generate_question(engine, request.aws_profile)

        logger.info("Generated auto-chat question")

        return GenerateQuestionResponse(success=True, question=question)

    except Exception as e:
        logger.exception("Failed to generate question")
        return GenerateQuestionResponse(
            success=False, error=f"Failed to generate question: {str(e)}"
        )


@router.get("/health/aws")
async def check_aws_health(engine: ChatEngine = Depends(get_chat_engine)):
    """
    Check AWS Bedrock connectivity and credentials status.

    Returns:
        AWS health status with details
    """
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
        from datetime import datetime, timezone

        # Get AWS profile from config
        config = engine.config
        aws_profile = config.aws_profile if hasattr(config, "aws_profile") else None

        result = {
            "status": "unknown",
            "profile": aws_profile or "default",
            "message": "",
            "details": {},
        }

        try:
            # Create session with profile
            session_kwargs = {}
            if aws_profile:
                session_kwargs["profile_name"] = aws_profile

            session = boto3.Session(**session_kwargs)

            # Check if credentials exist
            credentials = session.get_credentials()
            if not credentials:
                result["status"] = "error"
                result["message"] = "No AWS credentials found"
                result["details"]["error"] = "Credentials not configured"
                return result

            # Check if credentials are expired (for temporary credentials)
            if hasattr(credentials, "_expiry_time") and credentials._expiry_time:
                expiry_time = credentials._expiry_time
                if expiry_time.tzinfo is None:
                    expiry_time = expiry_time.replace(tzinfo=timezone.utc)

                now = datetime.now(timezone.utc)
                time_until_expiry = (expiry_time - now).total_seconds()

                if time_until_expiry < 0:
                    result["status"] = "error"
                    result["message"] = "AWS credentials expired"
                    result["details"]["expired"] = True
                    return result
                elif time_until_expiry < 300:  # Less than 5 minutes
                    result["status"] = "warning"
                    result["message"] = (
                        f"Credentials expiring in {int(time_until_expiry // 60)} minutes"
                    )
                    result["details"]["expiring_soon"] = True
                    result["details"]["seconds_until_expiry"] = int(time_until_expiry)
                else:
                    result["details"]["expires_in_seconds"] = int(time_until_expiry)

            # If we got here, credentials exist and aren't expired
            if result["status"] != "warning":  # Don't override warning status
                result["status"] = "healthy"
                result["message"] = "AWS credentials valid"

        except NoCredentialsError:
            result["status"] = "error"
            result["message"] = "No AWS credentials found"
            result["details"]["error"] = "NoCredentials"

        except Exception as e:
            result["status"] = "error"
            result["message"] = f"Error checking AWS: {str(e)}"
            result["details"]["error"] = str(e)

        return result

    except Exception as e:
        logger.exception("Failed to check AWS health")
        return {
            "status": "error",
            "profile": "unknown",
            "message": f"Health check failed: {str(e)}",
            "details": {"error": str(e)},
        }
