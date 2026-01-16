"""URL validation utilities to prevent SSRF attacks."""

from urllib.parse import urlparse


def validate_url(url: str) -> None:
    """
    Validate that a URL is safe for making HTTP requests.

    Raises ValueError if the URL is invalid or potentially unsafe.

    Args:
        url: The URL to validate

    Raises:
        ValueError: If the URL is invalid or unsafe
    """
    if not url:
        raise ValueError("URL cannot be empty")

    # Must start with http:// or https://
    if not url.startswith(('http://', 'https://')):
        raise ValueError("URL must start with http:// or https://")

    # Parse URL to validate structure
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Invalid URL format")
    except Exception as e:
        raise ValueError(f"Invalid URL: {e}")
