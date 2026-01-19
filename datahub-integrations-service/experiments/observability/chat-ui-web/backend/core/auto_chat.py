"""
Auto-Chat Question Generator - Stateless question generation using AWS Bedrock.

This module provides stateless question generation for auto-chat:
- Fetches DataHub metadata sketch from GMS
- Generates contextual questions using AWS Bedrock (Claude 3.5 Haiku)
- No state management - frontend drives the loop
"""

import sys
from pathlib import Path
from typing import Optional

from loguru import logger

# Add parent directory to path for chat_simulator imports
observability_dir = Path(__file__).parent.parent.parent.parent
if str(observability_dir) not in sys.path:
    sys.path.insert(0, str(observability_dir))

from chat_simulator import ConversationGenerator, DataHubSketch, DataHubSketcher


class AutoChatQuestionGenerator:
    """Stateless question generator for auto-chat."""

    def __init__(self):
        """Initialize question generator."""
        # Cache sketches and generators per GMS URL to avoid cross-instance contamination
        self.sketches: dict[str, DataHubSketch] = {}
        self.generators: dict[str, ConversationGenerator] = {}
        self.bedrock_client = None

    def _ensure_initialized(self, chat_engine, aws_profile: Optional[str] = None) -> None:
        """Ensure generator is initialized with sketch and Bedrock client."""
        import boto3

        # Get GMS URL as cache key
        gms_url = chat_engine.config.gms_url
        if not gms_url:
            raise ValueError("GMS URL not configured")

        # Only initialize once per GMS URL
        if gms_url in self.generators:
            logger.debug(f"Using cached generator for {gms_url}")
            return

        try:
            # Create Bedrock client with profile if specified (reuse if already created)
            if not self.bedrock_client:
                session_kwargs = {}
                if aws_profile:
                    session_kwargs["profile_name"] = aws_profile

                session = boto3.Session(**session_kwargs)
                self.bedrock_client = session.client(
                    service_name="bedrock-runtime", region_name="us-west-2"
                )

            # Create DataHub sketch for context (keyed by GMS URL)
            logger.info(f"Creating DataHub sketch for {gms_url}...")
            try:
                gms_token = chat_engine.config.gms_token

                if gms_token:
                    logger.info(f"Fetching DataHub metadata from GMS: {gms_url}")
                    sketcher = DataHubSketcher(gms_url, gms_token)
                    sketch = sketcher.create_sketch()
                    logger.info(
                        f"DataHub sketch created for {gms_url} with {len(sketch.platforms)} platforms, "
                        f"{len(sketch.top_datasets)} top datasets, "
                        f"{len(sketch.top_dashboards)} top dashboards"
                    )
                else:
                    logger.warning("GMS token not configured - creating empty sketch")
                    raise ValueError("GMS token not configured")
            except Exception as sketch_error:
                logger.warning(f"Failed to create sketch from GMS {gms_url}: {sketch_error}")
                logger.info("Creating empty sketch as fallback")
                # Fallback to empty sketch
                sketch = DataHubSketch(
                    entity_counts={},
                    platforms=[],
                    top_datasets=[],
                    top_dashboards=[],
                    sample_tags=[],
                    sample_glossary_terms=[],
                    sample_domains=[],
                )

            # Store sketch and create conversation generator (keyed by GMS URL)
            self.sketches[gms_url] = sketch
            self.generators[gms_url] = ConversationGenerator(self.bedrock_client, sketch)
            logger.info(f"Auto-chat question generator initialized for {gms_url}")

        except Exception as e:
            logger.error(f"Failed to initialize question generator: {e}")
            raise RuntimeError(
                f"Failed to initialize AWS Bedrock client. Make sure you have valid AWS credentials. Error: {e}"
            ) from e

    def generate_question(
        self, chat_engine, aws_profile: Optional[str] = None
    ) -> str:
        """
        Generate a contextual question about DataHub.

        Args:
            chat_engine: ChatEngine instance for GMS access
            aws_profile: Optional AWS profile name for Bedrock

        Returns:
            A generated question string

        Raises:
            RuntimeError: If initialization fails
        """
        # Ensure generator is initialized for this GMS URL
        self._ensure_initialized(chat_engine, aws_profile)

        # Get the generator for this specific GMS URL
        gms_url = chat_engine.config.gms_url
        if not gms_url or gms_url not in self.generators:
            raise RuntimeError(f"Generator not initialized for {gms_url}")

        try:
            # Generate question using the GMS-specific generator
            generator = self.generators[gms_url]
            question = generator.generate_initial_question()
            logger.info(f"Generated auto-chat question for {gms_url}: {question[:100]}...")
            return question

        except Exception as e:
            logger.error(f"Failed to generate question for {gms_url}: {e}")
            raise RuntimeError(f"Failed to generate question: {e}") from e


# Global singleton instance
_question_generator: Optional[AutoChatQuestionGenerator] = None


def get_question_generator() -> AutoChatQuestionGenerator:
    """Get or create the global question generator singleton."""
    global _question_generator
    if _question_generator is None:
        _question_generator = AutoChatQuestionGenerator()
    return _question_generator
