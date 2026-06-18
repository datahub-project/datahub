from datahub.ingestion.source.common.subtypes import DataObjectSubTypes
from datahub.ingestion.source.s3.data_object_utils import (
    classify_extension,
    guess_mime_type,
)


def test_classify_extension_known_media():
    assert classify_extension("mp4") == DataObjectSubTypes.VIDEO
    assert classify_extension("mp3") == DataObjectSubTypes.AUDIO
    assert classify_extension("png") == DataObjectSubTypes.IMAGE
    assert classify_extension("pdf") == DataObjectSubTypes.DOCUMENT
    assert classify_extension("zip") == DataObjectSubTypes.ARCHIVE


def test_classify_extension_unknown_falls_back_to_file():
    assert classify_extension("xyz") == DataObjectSubTypes.FILE


def test_guess_mime_type_prefers_content_type():
    assert guess_mime_type("a/b.mp4", "video/mp4") == "video/mp4"


def test_guess_mime_type_infers_from_extension():
    assert guess_mime_type("a/b.png", None) == "image/png"
    assert guess_mime_type("a/b.unknownext", None) is None
