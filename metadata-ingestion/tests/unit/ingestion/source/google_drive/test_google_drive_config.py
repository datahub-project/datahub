import pytest
from pydantic import ValidationError

from datahub.ingestion.source.google_drive.google_drive_config import (
    GoogleDriveSourceConfig,
)
from tests.test_helpers.google_drive_fakes import CREDENTIAL

FOLDER_ID = "1AbCdEfGhIjKlMnOp"


@pytest.mark.parametrize(
    "value",
    [
        FOLDER_ID,
        f"  {FOLDER_ID}  ",
        f"https://drive.google.com/drive/folders/{FOLDER_ID}",
        f"https://drive.google.com/drive/u/0/folders/{FOLDER_ID}?usp=sharing",
        f"https://drive.google.com/open?id={FOLDER_ID}",
    ],
)
def test_root_folder_ids_accept_ids_and_urls(value: str) -> None:
    config = GoogleDriveSourceConfig.model_validate({"root_folder_ids": [value]})
    assert config.root_folder_ids == [FOLDER_ID]


def test_root_folder_ids_are_deduplicated_in_order() -> None:
    other = "0AXyZsharedDriveId00"
    config = GoogleDriveSourceConfig.model_validate(
        {
            "root_folder_ids": [
                other,
                f"https://drive.google.com/drive/folders/{FOLDER_ID}",
                FOLDER_ID,
            ]
        }
    )
    assert config.root_folder_ids == [other, FOLDER_ID]


@pytest.mark.parametrize(
    "roots",
    [
        [],
        ["not a drive id!"],
        ["short"],
        ["https://drive.google.com/drive/folders/short"],
        ["https://drive.google.com/open?id=short"],
    ],
)
def test_invalid_root_folder_ids_rejected(roots: list) -> None:
    with pytest.raises(ValidationError):
        GoogleDriveSourceConfig.model_validate({"root_folder_ids": roots})


def test_delegated_user_requires_service_account_credential() -> None:
    with pytest.raises(ValidationError):
        GoogleDriveSourceConfig.model_validate(
            {"root_folder_ids": [FOLDER_ID], "delegated_user": "user@example.com"}
        )
    config = GoogleDriveSourceConfig.model_validate(
        {
            "root_folder_ids": [FOLDER_ID],
            "delegated_user": "user@example.com",
            "credential": CREDENTIAL,
        }
    )
    assert config.delegated_user == "user@example.com"


def test_inverted_text_length_range_rejected() -> None:
    with pytest.raises(ValidationError):
        GoogleDriveSourceConfig.model_validate(
            {
                "root_folder_ids": [FOLDER_ID],
                "min_text_length": 500,
                "max_text_length": 100,
            }
        )
    # 0 disables the upper limit, so any minimum is allowed.
    GoogleDriveSourceConfig.model_validate(
        {"root_folder_ids": [FOLDER_ID], "min_text_length": 500, "max_text_length": 0}
    )


def test_platform_instance_rejects_unsafe_characters() -> None:
    with pytest.raises(ValidationError):
        GoogleDriveSourceConfig.model_validate(
            {"root_folder_ids": [FOLDER_ID], "platform_instance": "team drive/1"}
        )


def test_parent_document_urn_must_be_a_document() -> None:
    with pytest.raises(ValidationError):
        GoogleDriveSourceConfig.model_validate(
            {
                "root_folder_ids": [FOLDER_ID],
                "parent_document_urn": "urn:li:dataset:(urn:li:dataPlatform:x,y,PROD)",
            }
        )
