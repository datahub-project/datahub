from typing import Any, Optional, cast
from unittest.mock import MagicMock
from urllib.parse import quote

from datahub.ingestion.graph.openapi import OpenApiAPI, RelationshipDirection


def _openapi(session: Optional[MagicMock] = None) -> OpenApiAPI:
    api = cast(Any, OpenApiAPI).__new__(OpenApiAPI)
    api._gms_server = "http://localhost:8080"
    api._session = session or MagicMock()
    api._get_generic = MagicMock()  # type: ignore[method-assign]
    api._post_generic = MagicMock()  # type: ignore[method-assign]
    return api


class TestGetRelatedEntities:
    def test_outgoing_calls_v3_endpoint_and_yields_destination(self) -> None:
        api = _openapi()
        urn = "urn:li:dataset:(urn:li:dataPlatform:hive,foo,PROD)"
        related = "urn:li:dataset:(urn:li:dataPlatform:hive,bar,PROD)"
        api._get_generic.return_value = {  # type: ignore[attr-defined]
            "results": [
                {
                    "relationshipType": "DownstreamOf",
                    "source": {"urn": urn},
                    "destination": {"urn": related},
                }
            ],
            "scrollId": None,
        }

        entities = list(
            api.get_related_entities(
                urn,
                relationship_types=["DownstreamOf"],
                direction=RelationshipDirection.OUTGOING,
            )
        )

        assert len(entities) == 1
        assert entities[0].urn == related
        assert entities[0].relationship_type == "DownstreamOf"

        encoded = quote(urn, safe="")
        expected_url = (
            f"http://localhost:8080/openapi/v3/relationship/dataset/{encoded}"
        )
        api._get_generic.assert_called_once()  # type: ignore[attr-defined]
        call_kwargs = api._get_generic.call_args.kwargs  # type: ignore[attr-defined]
        assert call_kwargs["url"] == expected_url
        assert call_kwargs["params"]["direction"] == "OUTGOING"
        assert call_kwargs["params"]["relationshipType[]"] == ["DownstreamOf"]
        assert call_kwargs["params"]["count"] == 200
        assert "scrollId" not in call_kwargs["params"]

    def test_incoming_yields_source_and_paginates(self) -> None:
        api = _openapi()
        urn = "urn:li:corpGroup:group_0"
        page1 = "urn:li:corpuser:user1"
        page2 = "urn:li:corpuser:user2"
        api._get_generic.side_effect = [  # type: ignore[attr-defined]
            {
                "results": [
                    {
                        "relationshipType": "IsMemberOfGroup",
                        "source": {"urn": page1},
                        "destination": {"urn": urn},
                    }
                ]
                * 200,
                "scrollId": "next",
            },
            {
                "results": [
                    {
                        "relationshipType": "IsMemberOfGroup",
                        "source": {"urn": page2},
                        "destination": {"urn": urn},
                    }
                ],
                "scrollId": None,
            },
        ]

        entities = list(
            api.get_related_entities(
                urn,
                relationship_types=["IsMemberOfGroup"],
                direction=RelationshipDirection.INCOMING,
            )
        )

        assert [e.urn for e in entities] == [page1] * 200 + [page2]
        assert api._get_generic.call_count == 2  # type: ignore[attr-defined]
        second_params = api._get_generic.call_args_list[1].kwargs["params"]  # type: ignore[attr-defined]
        assert second_params["scrollId"] == "next"

    def test_empty_results_short_circuits(self) -> None:
        api = _openapi()
        api._get_generic.return_value = {"results": [], "scrollId": "unused"}  # type: ignore[attr-defined]

        entities = list(
            api.get_related_entities(
                "urn:li:dataset:(urn:li:dataPlatform:hive,foo,PROD)",
                relationship_types=[],
                direction=RelationshipDirection.OUTGOING,
            )
        )

        assert entities == []
        params = api._get_generic.call_args.kwargs["params"]  # type: ignore[attr-defined]
        assert "relationshipType[]" not in params


class TestScrollRelationshipsEntityUrn:
    def test_entity_urn_passed_as_query_param(self) -> None:
        api = _openapi()
        api._post_generic.return_value = {"results": [], "scrollId": None}  # type: ignore[attr-defined]
        entity_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,foo,PROD)"

        api.scroll_relationships(
            entity_urn=entity_urn,
            direction=RelationshipDirection.INCOMING,
            count=10,
        )

        call_kwargs = api._post_generic.call_args.kwargs  # type: ignore[attr-defined]
        assert call_kwargs["params"]["entityUrn"] == entity_urn
        assert call_kwargs["params"]["direction"] == "INCOMING"
        assert (
            call_kwargs["url"] == "http://localhost:8080/openapi/v3/relationship/scroll"
        )
