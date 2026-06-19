from datahub.ingestion.source.cube.models import (
    CloudEntitiesResponse,
    CloudEntity,
    CloudReport,
    CloudWorkbook,
    CoreMetaResponse,
    CubeEntity,
    CubeReport,
    CubeWorkbook,
    merge_entities,
)


def test_report_extracts_referenced_entities_from_json_query() -> None:
    report = CubeReport.from_cloud(
        CloudReport.model_validate(
            {
                "id": 1,
                "publicId": "rpt1",
                "name": "r1",
                "jsonQuery": (
                    '{"measures":["orders_view.count"],'
                    '"dimensions":["orders_view.status"],'
                    '"timeDimensions":[{"dimension":"orders.created_at","granularity":"day"}],'
                    '"filters":[{"member":"customers.city","operator":"set"}]}'
                ),
                "user": {"id": 2, "email": "a@example.com"},
                "workbookId": 7,
            }
        )
    )
    # Distinct cube/view prefixes across every member-bearing clause, dedup-ed
    # while preserving first-seen order.
    assert report.referenced_entities == ["orders_view", "orders", "customers"]
    assert report.owner_email == "a@example.com"
    assert report.workbook_id == 7


def test_report_handles_missing_or_invalid_json_query() -> None:
    assert (
        CubeReport.from_cloud(
            CloudReport.model_validate({"id": 1, "name": "r1"})
        ).referenced_entities
        == []
    )
    assert (
        CubeReport.from_cloud(
            CloudReport.model_validate({"id": 1, "name": "r1", "jsonQuery": "not-json"})
        ).referenced_entities
        == []
    )


def test_workbook_collects_report_ids_from_published_dashboard() -> None:
    workbook = CubeWorkbook.from_cloud(
        CloudWorkbook.model_validate(
            {
                "id": 9,
                "name": "wb",
                "dashboardPublished": {"title": "WB", "description": "desc"},
                "publishedDashboard": {
                    "reportSnapshots": [
                        {"reportId": 1},
                        {"reportId": 2},
                        {"reportId": 1},
                    ]
                },
            }
        )
    )
    assert workbook.report_ids == [1, 2]
    assert workbook.title == "WB"
    assert workbook.description == "desc"


def test_from_core_cube_normalizes_members_and_alias() -> None:
    response = CoreMetaResponse.model_validate(
        {
            "cubes": [
                {
                    "name": "orders",
                    "title": "Orders",
                    "type": "view",
                    "measures": [
                        {
                            "name": "orders.count",
                            "title": "Count",
                            "type": "count",
                            "aggType": "count",
                            "aliasMember": "base_orders.count",
                        }
                    ],
                    "dimensions": [
                        {
                            "name": "orders.id",
                            "type": "number",
                            "primaryKey": True,
                        }
                    ],
                    "segments": [{"name": "orders.completed"}],
                }
            ]
        }
    )

    entity = CubeEntity.from_core_cube(response.cubes[0])

    assert entity.name == "orders"
    assert entity.is_view is True
    # Member names are stripped of their cube qualifier.
    assert entity.measures[0].name == "count"
    assert entity.measures[0].agg_type == "count"
    # aliasMember becomes a member reference for view -> cube lineage.
    assert entity.measures[0].member_references == ["base_orders.count"]
    assert entity.dimensions[0].name == "id"
    assert entity.dimensions[0].is_primary_key is True
    assert entity.segment_names == ["completed"]


def test_from_cloud_entity_captures_references() -> None:
    entity = CloudEntity.model_validate(
        {
            "type": "cube",
            "name": "orders",
            "table_references": [{"schema": "public", "table": "orders"}],
            "cube_references": ["users"],
            "measures": [
                {
                    "name": "count",
                    "type": "count",
                    "column_references": [
                        {"schema": "public", "table": "orders", "column": "id"}
                    ],
                }
            ],
            "dimensions": [
                {"name": "status", "type": "string", "member_references": ["base.s"]}
            ],
        }
    )

    normalized = CubeEntity.from_cloud_entity(entity)

    assert normalized.is_view is False
    assert normalized.cube_references == ["users"]
    assert normalized.table_references[0].table == "orders"
    assert normalized.measures[0].is_measure is True
    assert normalized.measures[0].column_references[0].column == "id"
    assert normalized.dimensions[0].member_references == ["base.s"]


def test_core_cube_sql_backticks_are_stripped() -> None:
    # Cube's /v1/meta wraps a cube's sql in backticks.
    response = CoreMetaResponse.model_validate(
        {
            "cubes": [
                {
                    "name": "orders",
                    "type": "cube",
                    "sql": "`SELECT * FROM public.orders`",
                }
            ]
        }
    )
    entity = CubeEntity.from_core_cube(response.cubes[0])
    assert entity.sql == "SELECT * FROM public.orders"


def test_column_reference_table_name_includes_database() -> None:
    entity = CubeEntity.from_cloud_entity(
        CloudEntity.model_validate(
            {
                "type": "cube",
                "name": "orders",
                "table_references": [{"schema": "public", "table": "orders"}],
            }
        )
    )
    ref = entity.table_references[0]
    assert ref.table_name() == "public.orders"
    assert ref.table_name(database="analytics") == "analytics.public.orders"


def test_core_cube_captures_structural_metadata_and_visibility() -> None:
    response = CoreMetaResponse.model_validate(
        {
            "cubes": [
                {
                    "name": "orders",
                    "type": "cube",
                    "fileName": "cubes/orders.yml",
                    "public": True,
                    "joins": [{"name": "users", "relationship": "belongsTo"}],
                    "hierarchies": [
                        {"name": "geo", "levels": ["orders.country", "orders.city"]}
                    ],
                    "folders": [{"name": "Core", "members": ["orders.count"]}],
                    "nestedFolders": [
                        {"name": "Nested", "members": [{"name": "orders.status"}]}
                    ],
                    "preAggregations": [{"name": "main_rollup"}, "secondary"],
                    "measures": [
                        {
                            "name": "orders.count",
                            "type": "number",
                            "aggType": "count",
                            "format": "percent",
                            "drillMembers": ["orders.id", "orders.status"],
                            "cumulative": True,
                            "public": False,
                        }
                    ],
                    "dimensions": [
                        {"name": "orders.status", "type": "string", "isVisible": False}
                    ],
                }
            ]
        }
    )

    entity = CubeEntity.from_core_cube(response.cubes[0])

    assert entity.file_name == "cubes/orders.yml"
    assert entity.joins[0].name == "users"
    assert entity.joins[0].relationship == "belongsTo"
    assert entity.hierarchies[0].levels == ["country", "city"]
    # Flat and nested folders are combined; member names are unqualified.
    assert {f.name for f in entity.folders} == {"Core", "Nested"}
    assert entity.pre_aggregation_names == ["main_rollup", "secondary"]

    measure = entity.measures[0]
    assert measure.format == "percent"
    assert measure.drill_members == ["id", "status"]
    assert measure.cumulative is True
    # `public: false` / `isVisible: false` mark members hidden.
    assert measure.is_hidden is True
    assert entity.dimensions[0].is_hidden is True


def test_hidden_cube_flag() -> None:
    response = CoreMetaResponse.model_validate(
        {"cubes": [{"name": "secret", "type": "cube", "public": False}]}
    )
    assert CubeEntity.from_core_cube(response.cubes[0]).is_hidden is True


def test_visible_members_filtering() -> None:
    response = CoreMetaResponse.model_validate(
        {
            "cubes": [
                {
                    "name": "orders",
                    "type": "cube",
                    "measures": [{"name": "orders.count", "public": False}],
                    "dimensions": [{"name": "orders.status"}],
                }
            ]
        }
    )
    entity = CubeEntity.from_core_cube(response.cubes[0])
    assert [m.name for m in entity.visible_members(include_hidden=False)] == ["status"]
    assert len(entity.visible_members(include_hidden=True)) == 2


def test_merge_entities_overlays_cloud_lineage_onto_core_structural() -> None:
    core = [
        CubeEntity.from_core_cube(c)
        for c in CoreMetaResponse.model_validate(
            {
                "cubes": [
                    {
                        "name": "orders",
                        "type": "cube",
                        "fileName": "model/cubes/orders.yml",
                        "joins": [{"name": "users", "relationship": "many_to_one"}],
                        "measures": [
                            {
                                "name": "orders.count",
                                "type": "count",
                                "format": "number",
                            }
                        ],
                        "dimensions": [{"name": "orders.status", "type": "string"}],
                    }
                ]
            }
        ).cubes
    ]
    cloud = [
        CubeEntity.from_cloud_entity(e)
        for e in CloudEntitiesResponse.model_validate(
            {
                "data": {
                    "entities": [
                        {
                            "type": "cube",
                            "name": "orders",
                            "table_references": [
                                {"schema": "public", "table": "orders"}
                            ],
                            "cube_references": ["users"],
                            "measures": [
                                {
                                    "name": "count",
                                    "type": "count",
                                    "column_references": [
                                        {
                                            "schema": "public",
                                            "table": "orders",
                                            "column": "id",
                                        }
                                    ],
                                }
                            ],
                            "dimensions": [{"name": "status", "type": "string"}],
                        },
                        {
                            "type": "cube",
                            "name": "hidden_only_in_cloud",
                            "measures": [],
                            "dimensions": [],
                        },
                    ]
                }
            }
        ).data.entities
    ]

    merged = {e.name: e for e in merge_entities(core, cloud)}

    # Structural metadata from /v1/meta is preserved.
    orders = merged["orders"]
    assert orders.file_name == "model/cubes/orders.yml"
    assert [j.name for j in orders.joins] == ["users"]
    assert orders.measures[0].format == "number"
    # Lineage from the Metadata API is overlaid.
    assert [t.table for t in orders.table_references] == ["orders"]
    count = next(m for m in orders.measures if m.name == "count")
    assert [c.column for c in count.column_references] == ["id"]
    # Entities only present in the Metadata API are surfaced too.
    assert "hidden_only_in_cloud" in merged
