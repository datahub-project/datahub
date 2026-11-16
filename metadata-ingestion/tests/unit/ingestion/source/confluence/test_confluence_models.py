from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.confluence.confluence_models import ConfluencePage


def test_page_flattens_the_content_api_payload() -> None:
    page = ConfluencePage.model_validate(
        {
            "id": 123,
            "title": "Runbook",
            "space": {"key": "OPS", "name": "Operations"},
            "ancestors": [
                {"id": "1", "title": "Home", "type": "page"},
                {"id": "2", "title": "Guides", "type": "folder"},
            ],
            "body": {"storage": {"value": "<p>Body</p>"}},
            "_links": {"webui": "/spaces/OPS/pages/123/Runbook"},
            "version": {"when": "2024-05-01T10:00:00.000Z"},
            "history": {"createdDate": "2024-01-01T09:00:00.000Z"},
        }
    )

    assert page.id == "123"
    assert page.space.key == "OPS"
    assert page.parent_id == "2"
    assert page.ancestors[1].is_folder
    assert page.storage == "<p>Body</p>"
    assert page.webui == "/spaces/OPS/pages/123/Runbook"
    assert page.last_modified == datetime(2024, 5, 1, 10, tzinfo=timezone.utc)
    assert page.created == datetime(2024, 1, 1, 9, tzinfo=timezone.utc)


def test_page_tolerates_missing_and_malformed_fields() -> None:
    page = ConfluencePage.model_validate(
        {
            "id": "5",
            "spaceKey": "TEAM",
            "ancestors": [{"title": "no id"}, "not a dict", {"id": "4"}],
            "version": {"when": "not a date"},
        }
    )

    assert page.space.key == "TEAM"
    assert [a.id for a in page.ancestors] == ["4"]
    assert page.storage == ""
    assert page.last_modified is None


def test_root_page_has_no_parent() -> None:
    assert ConfluencePage.model_validate({"id": "1"}).parent_id is None


def test_page_without_id_is_rejected() -> None:
    with pytest.raises(ValidationError):
        ConfluencePage.model_validate({"title": "No id"})
