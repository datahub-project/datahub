import mimetypes
from typing import Optional

from datahub.ingestion.source.common.subtypes import DataObjectSubTypes

_EXTENSION_TO_SUBTYPE = {
    # Audio
    "mp3": DataObjectSubTypes.AUDIO,
    "wav": DataObjectSubTypes.AUDIO,
    "flac": DataObjectSubTypes.AUDIO,
    "aac": DataObjectSubTypes.AUDIO,
    "ogg": DataObjectSubTypes.AUDIO,
    "m4a": DataObjectSubTypes.AUDIO,
    # Video
    "mp4": DataObjectSubTypes.VIDEO,
    "mov": DataObjectSubTypes.VIDEO,
    "avi": DataObjectSubTypes.VIDEO,
    "mkv": DataObjectSubTypes.VIDEO,
    "webm": DataObjectSubTypes.VIDEO,
    "wmv": DataObjectSubTypes.VIDEO,
    # Image
    "jpg": DataObjectSubTypes.IMAGE,
    "jpeg": DataObjectSubTypes.IMAGE,
    "png": DataObjectSubTypes.IMAGE,
    "gif": DataObjectSubTypes.IMAGE,
    "tiff": DataObjectSubTypes.IMAGE,
    "bmp": DataObjectSubTypes.IMAGE,
    "svg": DataObjectSubTypes.IMAGE,
    "webp": DataObjectSubTypes.IMAGE,
    # Document / text
    "pdf": DataObjectSubTypes.DOCUMENT,
    "doc": DataObjectSubTypes.DOCUMENT,
    "docx": DataObjectSubTypes.DOCUMENT,
    "ppt": DataObjectSubTypes.DOCUMENT,
    "pptx": DataObjectSubTypes.DOCUMENT,
    "rtf": DataObjectSubTypes.DOCUMENT,
    "txt": DataObjectSubTypes.TEXT,
    "md": DataObjectSubTypes.TEXT,
    # Archive
    "zip": DataObjectSubTypes.ARCHIVE,
    "tar": DataObjectSubTypes.ARCHIVE,
    "gz": DataObjectSubTypes.ARCHIVE,
    "7z": DataObjectSubTypes.ARCHIVE,
    "rar": DataObjectSubTypes.ARCHIVE,
}


def classify_extension(ext: str) -> DataObjectSubTypes:
    """Map a file extension (no leading dot, any case) to a dataObject subtype."""
    return _EXTENSION_TO_SUBTYPE.get(ext.lower().lstrip("."), DataObjectSubTypes.FILE)


def guess_mime_type(path: str, content_type: Optional[str]) -> Optional[str]:
    """Prefer the storage-reported content type; otherwise infer from the extension."""
    if content_type:
        return content_type
    guessed, _ = mimetypes.guess_type(path)
    return guessed
