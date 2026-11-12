from unittest import mock

from datahub.ingestion.source.quicksight.quicksight_api import QuickSightAPI
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)


def _api(client: mock.MagicMock) -> QuickSightAPI:
    config = QuickSightSourceConfig.model_validate(
        {"aws_region": "us-east-1", "aws_account_id": "123456789012"}
    )
    with mock.patch.object(
        QuickSightSourceConfig, "get_quicksight_client", return_value=client
    ):
        return QuickSightAPI(config, QuickSightSourceReport())


def test_list_folder_members_follows_next_token_across_pages():
    # list_folder_members has no boto3 paginator, so the connector follows
    # NextToken manually. A folder with >100 members spans multiple pages; this
    # guards against silently dropping membership beyond page 1.
    client = mock.MagicMock()
    client.list_folder_members.side_effect = [
        {
            "FolderMemberList": [{"MemberId": "a"}, {"MemberId": "b"}],
            "NextToken": "page-2",
        },
        {"FolderMemberList": [{"MemberId": "c"}]},
    ]
    api = _api(client)

    members = list(api.list_folder_members("folder-1"))

    assert [m["MemberId"] for m in members] == ["a", "b", "c"]
    assert client.list_folder_members.call_count == 2
    # The second request must forward the NextToken returned by page 1.
    assert client.list_folder_members.call_args_list[1].kwargs["NextToken"] == "page-2"
