"""Unit tests for MicroStrategy source connector."""

from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.microstrategy.client import (
    MicroStrategyClient,
    MicroStrategyProjectUnavailableError,
)
from datahub.ingestion.source.microstrategy.config import (
    MicroStrategyConfig,
    MicroStrategyConnectionConfig,
)
from datahub.ingestion.source.microstrategy.constants import ISERVER_PROJECT_UNAVAILABLE
from datahub.ingestion.source.microstrategy.source import (
    ISERVER_CUBE_NOT_PUBLISHED,
    ISERVER_DYNAMIC_SOURCING_CUBE,
    FolderKey,
    MicroStrategySource,
    ProjectKey,
    _extract_tables_from_sql,
    _is_classcast_error,
    _is_iserver_error,
    _parse_database_from_connection_string,
    _parse_schema_from_connection_string,
)


def create_search_objects_mock(
    dashboards: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    reports: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    cubes: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Callable[[str, int], List[Dict[str, Any]]]:
    """
    Helper to create search_objects mock function.

    Args:
        dashboards: Dict mapping project_id to list of dashboards
        reports: Dict mapping project_id to list of reports
        cubes: Dict mapping project_id to list of cubes

    Returns:
        Mock function compatible with search_objects(project_id, object_type)
    """
    dashboards = dashboards or {}
    reports = reports or {}
    cubes = cubes or {}

    def search_objects_side_effect(
        project_id: str, object_type: int
    ) -> List[Dict[str, Any]]:
        # type=776 is cubes (search), type=3 is reports, type=55 is dashboards
        if object_type == 55:  # Dashboards
            return dashboards.get(project_id, [])
        elif object_type == 3:  # Reports
            return reports.get(project_id, [])
        elif object_type == 776:  # Cubes
            return cubes.get(project_id, [])
        return []

    return search_objects_side_effect


class TestMicroStrategyConfigValidation:
    """Test configuration validation logic."""

    def test_base_url_validation_rejects_non_http(self):
        """Test that base_url must start with http:// or https://."""
        with pytest.raises(ValueError, match="base_url must start with http"):
            MicroStrategyConnectionConfig(base_url="ftp://invalid.com")

    def test_base_url_validation_accepts_http(self):
        """Test that http:// URLs are accepted."""
        config = MicroStrategyConnectionConfig(
            base_url="http://localhost:8080", use_anonymous=True
        )
        assert config.base_url == "http://localhost:8080"

    def test_base_url_trailing_slashes_removed(self):
        """Test that trailing slashes are removed from base_url."""
        config = MicroStrategyConnectionConfig(
            base_url="https://demo.microstrategy.com///", use_anonymous=True
        )
        assert config.base_url == "https://demo.microstrategy.com"


class TestContainerKeyURNGeneration:
    """Test that custom ContainerKey subclasses generate unique URNs."""

    def test_project_keys_generate_unique_urns(self):
        """Test that different projects generate different URNs."""
        project_key1 = ProjectKey(
            project="project_a", platform="microstrategy", instance=None, env="PROD"
        )
        project_key2 = ProjectKey(
            project="project_b", platform="microstrategy", instance=None, env="PROD"
        )

        urn1 = project_key1.as_urn()
        urn2 = project_key2.as_urn()

        # URNs should be different for different projects
        assert urn1 != urn2
        # Both should be valid container URNs
        assert urn1.startswith("urn:li:container:")
        assert urn2.startswith("urn:li:container:")

    def test_folder_keys_with_same_id_different_projects_generate_unique_urns(self):
        """Test that folders with same ID in different projects get unique URNs."""
        folder_key1 = FolderKey(
            project="project_a",
            folder="folder_1",
            platform="microstrategy",
            instance=None,
            env="PROD",
        )
        folder_key2 = FolderKey(
            project="project_b",
            folder="folder_1",
            platform="microstrategy",
            instance=None,
            env="PROD",
        )

        urn1 = folder_key1.as_urn()
        urn2 = folder_key2.as_urn()

        # URNs should be different because they're in different projects
        assert urn1 != urn2


class TestProjectFiltering:
    """Test project filtering logic."""

    def test_project_pattern_filters_projects(self):
        """Test that project patterns correctly filter projects."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "project_pattern": {
                "allow": ["^Production.*"],
                "deny": [".*Test$"],
            },
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "1", "name": "Production Main", "status": 0},
                {"id": "2", "name": "Production Test", "status": 0},  # filtered by deny
                {"id": "3", "name": "Dev Project", "status": 0},  # filtered by allow
                {"id": "4", "name": "Production Analytics", "status": 0},
            ]
            mock_client.get_folders.return_value = []
            mock_client.search_objects.side_effect = create_search_objects_mock()

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Check that only "Production Main" and "Production Analytics" were processed
            # Each container generates multiple aspects (containerProperties, status, etc.)
            # So we need to count unique container URNs
            container_urns = set(
                wu.metadata.entityUrn
                for wu in workunits
                if hasattr(wu.metadata, "entityUrn")
                and "container" in str(wu.metadata.entityUrn)
            )

            # Should have 2 unique project containers (Production Main, Production Analytics)
            assert len(container_urns) == 2


class TestCubeRegistryResolution:
    """Test cross-project cube registry resolution."""

    def test_cube_registry_resolves_cross_project_references(self):
        """Test that cubes from different projects can be resolved for lineage."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_lineage": True,
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            # Project A has a report that references cube from Project B
            mock_client.get_projects.return_value = [
                {"id": "project_a", "name": "Project A", "status": 0},
                {"id": "project_b", "name": "Project B", "status": 0},
            ]

            mock_client.search_objects.side_effect = create_search_objects_mock(
                cubes={
                    "project_b": [
                        {"id": "cube_123", "name": "Sales Cube", "description": ""}
                    ]
                },
                reports={
                    "project_a": [
                        {
                            "id": "report_1",
                            "name": "Sales Report",
                            "description": "",
                            "type": 3,
                            "subtype": 3,
                            "dataSource": {
                                "id": "cube_123"
                            },  # References cube from Project B
                        }
                    ]
                },
            )
            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Verify cube was registered
            assert "cube_123" in source.cube_registry
            assert source.cube_registry["cube_123"]["project_id"] == "project_b"

            # Verify report has lineage (inputs should contain the cube URN)
            chart_info_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "ChartInfoClass"
            ]

            assert len(chart_info_workunits) > 0
            chart_info = chart_info_workunits[0].metadata.aspect  # type: ignore
            assert chart_info is not None
            assert chart_info.customProperties["report_type"] == "3"  # type: ignore

            # Check that lineage includes the cube
            assert hasattr(chart_info, "inputs")
            assert len(chart_info.inputs) > 0  # type: ignore
            assert any("cube_123" in input_urn for input_urn in chart_info.inputs)  # type: ignore


class TestOwnershipExtraction:
    """Test ownership extraction logic."""

    def test_ownership_handles_string_owner(self):
        """Test that ownership aspect is created when owner is a string."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_ownership": True,
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "Test Project", "status": 0}
            ]
            mock_client.search_objects.side_effect = create_search_objects_mock(
                dashboards={
                    "project_1": [
                        {
                            "id": "dashboard_1",
                            "name": "Test Dashboard",
                            "description": "",
                            "owner": "john.doe",  # String owner
                        }
                    ]
                }
            )
            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Find ownership workunit
            ownership_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "OwnershipClass"
            ]

            assert len(ownership_workunits) > 0
            ownership = ownership_workunits[0].metadata.aspect  # type: ignore
            assert ownership is not None
            assert len(ownership.owners) == 1  # type: ignore
            assert "john.doe" in ownership.owners[0].owner  # type: ignore

    def test_ownership_handles_dict_owner(self):
        """Test that ownership aspect is created when owner is a dict with username."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_ownership": True,
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "Test Project", "status": 0}
            ]
            mock_client.search_objects.side_effect = create_search_objects_mock(
                dashboards={
                    "project_1": [
                        {
                            "id": "dashboard_1",
                            "name": "Test Dashboard",
                            "description": "",
                            "owner": {
                                "username": "jane.smith",
                                "email": "jane@example.com",
                            },
                        }
                    ]
                }
            )
            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Find ownership workunit
            ownership_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "OwnershipClass"
            ]

            assert len(ownership_workunits) > 0
            ownership = ownership_workunits[0].metadata.aspect  # type: ignore
            assert ownership is not None
            assert len(ownership.owners) == 1  # type: ignore
            assert "jane.smith" in ownership.owners[0].owner  # type: ignore


class TestDashboardVisualizationExtraction:
    """Test visualization extraction from dashboard definitions."""

    def test_visualization_ids_extracted_from_dashboard_chapters(self):
        """Test that visualization IDs are correctly extracted from dashboard definition."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_lineage": True,
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "Test Project", "status": 0}
            ]
            mock_client.search_objects.side_effect = create_search_objects_mock(
                dashboards={
                    "project_1": [
                        {
                            "id": "dashboard_1",
                            "name": "Test Dashboard",
                            "description": "",
                            "subtype": 14336,
                        }
                    ]
                }
            )

            # Modern dossier: chapters → pages → visualizations
            mock_client.get_dossier_definition.return_value = {
                "chapters": [
                    {
                        "name": "Chapter 1",
                        "pages": [
                            {
                                "visualizations": [
                                    {"key": "viz_1", "type": "chart"},
                                    {"key": "viz_2", "type": "grid"},
                                ],
                            }
                        ],
                    },
                    {
                        "name": "Chapter 2",
                        "pages": [
                            {
                                "visualizations": [
                                    {"key": "viz_3", "type": "map"},
                                ],
                            }
                        ],
                    },
                ]
            }

            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Find dashboard info workunit
            dashboard_info_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "DashboardInfoClass"
            ]

            assert len(dashboard_info_workunits) > 0
            dashboard_info = dashboard_info_workunits[0].metadata.aspect  # type: ignore
            assert dashboard_info is not None

            # Verify 3 chart URNs were created
            # SDK V2 Dashboard stores charts as chartEdges (deprecated charts field is empty)
            chart_urns = [
                edge.destinationUrn
                for edge in (dashboard_info.chartEdges or [])  # type: ignore[union-attr]
            ]
            assert len(chart_urns) == 3
            assert all("viz_" in chart_urn for chart_urn in chart_urns)

            mock_client.get_dossier_definition.assert_called_once_with(
                "dashboard_1", "project_1"
            )

    def test_empty_dashboard_definition_handled_gracefully(self):
        """Test that dashboards with no visualizations don't crash the source."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_lineage": True,
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "Test Project", "status": 0}
            ]
            mock_client.search_objects.side_effect = create_search_objects_mock(
                dashboards={
                    "project_1": [
                        {
                            "id": "dashboard_1",
                            "name": "Empty Dashboard",
                            "description": "",
                            "subtype": 14336,
                        }
                    ]
                }
            )

            mock_client.get_dossier_definition.return_value = {"chapters": []}

            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Should still create dashboard workunit, just with empty charts list
            dashboard_info_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "DashboardInfoClass"
            ]

            assert len(dashboard_info_workunits) > 0
            dashboard_info = dashboard_info_workunits[0].metadata.aspect  # type: ignore
            assert dashboard_info is not None
            # SDK V2 Dashboard stores charts as chartEdges (deprecated charts field is empty)
            assert len(dashboard_info.chartEdges or []) == 0  # type: ignore

            mock_client.get_dossier_definition.assert_called_once_with(
                "dashboard_1", "project_1"
            )


class TestCubeSchemaExtraction:
    """Test cube schema extraction logic."""

    def test_cube_schema_includes_attributes_and_metrics(self):
        """Test that cube schema correctly extracts attributes and metrics."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_cube_schema": True,
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "Test Project", "status": 0}
            ]
            mock_client.search_objects.side_effect = create_search_objects_mock(
                cubes={
                    "project_1": [
                        {
                            "id": "cube_1",
                            "name": "Sales Cube",
                            "description": "Sales data",
                        }
                    ]
                }
            )

            # Schema lives under definition.availableObjects (MSTR REST cube payload)
            mock_client.get_cube.return_value = {
                "definition": {
                    "availableObjects": {
                        "attributes": [
                            {
                                "id": "attr_1",
                                "name": "Region",
                                "description": "Sales region",
                                "forms": [
                                    {
                                        "name": "ID",
                                        "dataType": "varChar",
                                        "baseFormCategory": "DESC",
                                    }
                                ],
                            },
                            {
                                "id": "attr_2",
                                "name": "Product",
                                "description": "Product name",
                                "forms": [
                                    {
                                        "name": "ID",
                                        "dataType": "varChar",
                                        "baseFormCategory": "DESC",
                                    }
                                ],
                            },
                        ],
                        "metrics": [
                            {
                                "id": "metric_1",
                                "name": "Revenue",
                                "description": "Total revenue",
                            },
                            {
                                "id": "metric_2",
                                "name": "Units",
                                "description": "Units sold",
                            },
                        ],
                    }
                }
            }
            mock_client.get_cube_sql_view.return_value = 'SELECT 1 FROM "S"."T"'

            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Find schema metadata workunit
            schema_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "SchemaMetadataClass"
            ]

            assert len(schema_workunits) > 0
            schema_metadata = schema_workunits[0].metadata.aspect  # type: ignore
            assert schema_metadata is not None

            # Verify fields include both attributes and metrics
            assert len(schema_metadata.fields) == 4  # type: ignore

            field_paths = [field.fieldPath for field in schema_metadata.fields]  # type: ignore
            assert "Region" in field_paths
            assert "Product" in field_paths
            assert "Revenue" in field_paths
            assert "Units" in field_paths

            view_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "ViewPropertiesClass"
            ]
            assert len(view_workunits) == 1
            assert "FROM" in view_workunits[0].metadata.aspect.viewLogic  # type: ignore

            # Verify native data types
            field_types = {
                field.fieldPath: field.nativeDataType
                for field in schema_metadata.fields  # type: ignore
            }
            assert field_types["Region"] == "attribute:DESC"
            assert field_types["Revenue"] == "metric"

    def test_cube_schema_extraction_failure_continues_ingestion(self):
        """Test that cube schema extraction failure doesn't stop ingestion."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_cube_schema": True,
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "Test Project", "status": 0}
            ]
            mock_client.search_objects.side_effect = create_search_objects_mock(
                cubes={
                    "project_1": [
                        {"id": "cube_1", "name": "Sales Cube", "description": ""}
                    ]
                }
            )

            # Mock schema extraction to fail — production code calls get_cube(), not get_cube_schema()
            mock_client.get_cube.side_effect = Exception(
                "Permission denied to cube schema"
            )
            mock_client.get_cube_sql_view.return_value = ""

            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Should still emit dataset properties workunit for cube
            dataset_properties_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "DatasetPropertiesClass"
            ]

            assert len(dataset_properties_workunits) > 0

            # Should NOT emit schema metadata workunit
            schema_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "SchemaMetadataClass"
            ]

            assert len(schema_workunits) == 0


class TestURLConstruction:
    """Test external URL construction logic."""

    def test_dashboard_url_construction(self):
        """Test that dashboard URLs are correctly constructed."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com/MicroStrategyLibrary",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        source = MicroStrategySource(config, ctx)
        url = source._build_dashboard_url("dashboard_123", "project_456")

        assert (
            url
            == "https://demo.microstrategy.com/MicroStrategyLibrary/app/project_456/dashboard_123"
        )

    def test_report_url_construction(self):
        """Test that report URLs are correctly constructed."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com/MicroStrategyLibrary",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        source = MicroStrategySource(config, ctx)
        url = source._build_report_url("report_789", "project_456")

        assert (
            url
            == "https://demo.microstrategy.com/MicroStrategyLibrary/app/project_456/report_789"
        )

    def test_url_construction_handles_trailing_slashes(self):
        """Test that trailing slashes in base_url don't create malformed URLs."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com/MicroStrategyLibrary///",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        source = MicroStrategySource(config, ctx)
        url = source._build_dashboard_url("dashboard_123", "project_456")
        assert url is not None

        # Should not have double slashes
        assert "//" not in url.replace("https://", "")  # type: ignore
        assert url.endswith("/project_456/dashboard_123")  # type: ignore


class TestErrorHandling:
    """Test error handling and recovery."""

    def test_project_extraction_failure_continues_to_next_project(self):
        """Test that failure to extract one project doesn't stop ingestion."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "Project 1", "status": 0},
                {"id": "project_2", "name": "Project 2", "status": 0},
            ]

            def search_objects_with_error(
                project_id: str, object_type: int
            ) -> List[Dict[str, Any]]:
                if project_id == "project_1" and object_type == 55:  # Dashboards
                    raise Exception("Permission denied")
                if project_id == "project_2" and object_type == 55:
                    return [
                        {"id": "dashboard_1", "name": "Dashboard 1", "description": ""}
                    ]
                return []

            mock_client.search_objects.side_effect = search_objects_with_error
            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Should still emit containers for both projects
            # Count unique container URNs (each container generates multiple aspects)
            container_urns = set(
                wu.metadata.entityUrn
                for wu in workunits
                if hasattr(wu.metadata, "entityUrn")
                and "container" in str(wu.metadata.entityUrn)
            )
            assert len(container_urns) == 2

            # Should emit dashboard from project_2
            dashboard_info_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "DashboardInfoClass"
            ]
            assert len(dashboard_info_workunits) == 1


class TestLoadedProjectFiltering:
    """Projects with status != 0 are skipped unless include_unloaded_projects is set."""

    def test_skips_unloaded_projects_by_default(self):
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "loaded", "name": "Loaded", "status": 0},
                {"id": "idle", "name": "Idle", "status": 2},
            ]
            mock_client.get_folders.return_value = []
            mock_client.search_objects.side_effect = create_search_objects_mock()

            source = MicroStrategySource(config, ctx)
            list(source.get_workunits())

            mock_client.get_folders.assert_called_once_with("loaded")

    def test_include_unloaded_projects_processes_all_matching(self):
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_unloaded_projects": True,
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "loaded", "name": "Loaded", "status": 0},
                {"id": "idle", "name": "Idle", "status": 2},
            ]
            mock_client.get_folders.return_value = []
            mock_client.search_objects.side_effect = create_search_objects_mock()

            source = MicroStrategySource(config, ctx)
            list(source.get_workunits())

            assert mock_client.get_folders.call_count == 2


class TestMicroStrategyClientErrors:
    def test_request_raises_project_unavailable_on_iserver_body(self):
        cfg = MicroStrategyConnectionConfig(
            base_url="https://mstr.example.com",
            use_anonymous=True,
        )
        client = MicroStrategyClient(cfg)
        client.auth_token = "tok"
        client.token_created_at = datetime.now()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"{}"
        mock_resp.json.return_value = {
            "iServerCode": ISERVER_PROJECT_UNAVAILABLE,
            "message": "Project not loaded",
            "ticketId": "abc",
        }

        with (
            patch.object(client.session, "request", return_value=mock_resp),
            pytest.raises(MicroStrategyProjectUnavailableError) as excinfo,
        ):
            client._request("GET", "/api/projects")

        assert excinfo.value.i_server_code == ISERVER_PROJECT_UNAVAILABLE
        assert excinfo.value.ticket_id == "abc"

    def test_get_dossier_definition_empty_on_classcast_500(self):
        cfg = MicroStrategyConnectionConfig(
            base_url="https://mstr.example.com",
            use_anonymous=True,
        )
        client = MicroStrategyClient(cfg)
        client.auth_token = "tok"
        client.token_created_at = datetime.now()

        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.content = b"{}"
        mock_resp.json.return_value = {"message": "Cannot be cast to DossierBean"}

        with patch.object(client.session, "request", return_value=mock_resp):
            assert client.get_dossier_definition("d1", "p1") == {}


class TestWarehouseLineageEmission:
    def test_model_cube_physical_tables_emit_upstream_lineage(self):
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_lineage": True,
            "include_warehouse_lineage": True,
            "warehouse_lineage_database": "db",
            "warehouse_lineage_schema": "public",
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "P1", "status": 0}
            ]
            # Mock project datasources so platform auto-detection resolves to snowflake
            mock_client.get_project_datasources.return_value = [
                {
                    "id": "ds1",
                    "name": "Snowflake Prod",
                    "datasourceType": "normal",
                    "database": {"type": "snow_flake"},
                    "dbms": {"name": "Snowflake"},
                }
            ]
            mock_client.search_objects.side_effect = create_search_objects_mock(
                cubes={
                    "project_1": [
                        {"id": "cube_1", "name": "Cube", "description": ""},
                    ]
                }
            )
            mock_client.get_folders.return_value = []
            mock_client.get_cube_sql_view.return_value = (
                'SELECT a FROM "analytics"."fact_sales" x'
            )

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            upstream_wus = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "UpstreamLineageClass"
            ]
            assert len(upstream_wus) == 1
            lineage = upstream_wus[0].metadata.aspect  # type: ignore[union-attr]
            assert len(lineage.upstreams) == 1  # type: ignore[union-attr]
            urn = lineage.upstreams[0].dataset  # type: ignore[union-attr]
            assert "snowflake" in urn
            # 2-part SQL name "analytics"."fact_sales" + warehouse_lineage_database="db"
            # should produce 3-part: db.analytics.fact_sales
            assert "db.analytics.fact_sales" in urn

    def test_tables_to_urns_emits_warning_when_platform_undetected(self) -> None:
        """_tables_to_urns returns [] and records a report warning when no platform is detected."""
        config = MicroStrategyConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://demo.microstrategy.com",
                    "use_anonymous": True,
                },
                "include_warehouse_lineage": True,
            }
        )
        ctx = PipelineContext(run_id="test-no-platform")
        source = MicroStrategySource(config, ctx)
        # _warehouse_platform starts as None — no detection has run
        assert source._warehouse_platform is None

        result = source._tables_to_urns(["analytics.fact_sales"])

        assert result == []
        # title is the grouping key set via title="warehouse-lineage-skipped"
        warning_titles = [w.title for w in source.report.warnings]
        assert "warehouse-lineage-skipped" in warning_titles


class TestQualifyTableName:
    """Tests for _qualify_table_name — ensures MSTR SQL table names are
    qualified to the depth expected by DataHub URNs (e.g. 3-part for Snowflake)."""

    @staticmethod
    def _make_source(**overrides: Any) -> "MicroStrategySource":
        base: Dict[str, Any] = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
        }
        base.update(overrides)
        config = MicroStrategyConfig.model_validate(base)
        ctx = PipelineContext(run_id="qualify-test")
        return MicroStrategySource(config, ctx)

    def test_bare_table_with_db_and_schema(self) -> None:
        source = self._make_source(
            warehouse_lineage_database="MY_DB",
            warehouse_lineage_schema="MY_SCHEMA",
        )
        assert source._qualify_table_name("FACT_SALES") == "MY_DB.MY_SCHEMA.FACT_SALES"

    def test_bare_table_with_schema_only(self) -> None:
        source = self._make_source(warehouse_lineage_schema="MY_SCHEMA")
        assert source._qualify_table_name("FACT_SALES") == "MY_SCHEMA.FACT_SALES"

    def test_bare_table_no_config(self) -> None:
        source = self._make_source()
        assert source._qualify_table_name("FACT_SALES") == "FACT_SALES"

    def test_two_part_prepends_database(self) -> None:
        """MSTR SQL typically produces SCHEMA.TABLE — database should be prepended."""
        source = self._make_source(warehouse_lineage_database="ANALYTICS")
        assert (
            source._qualify_table_name("XRBIA_DM.FACT_SALES")
            == "ANALYTICS.XRBIA_DM.FACT_SALES"
        )

    def test_two_part_no_database_unchanged(self) -> None:
        """Without warehouse_lineage_database, 2-part names pass through."""
        source = self._make_source(warehouse_lineage_schema="IGNORED")
        assert (
            source._qualify_table_name("XRBIA_DM.FACT_SALES") == "XRBIA_DM.FACT_SALES"
        )

    def test_three_part_unchanged(self) -> None:
        """Already fully qualified — should not be modified."""
        source = self._make_source(
            warehouse_lineage_database="OTHER_DB",
            warehouse_lineage_schema="OTHER_SCHEMA",
        )
        assert (
            source._qualify_table_name("MY_DB.MY_SCHEMA.FACT_SALES")
            == "MY_DB.MY_SCHEMA.FACT_SALES"
        )


class TestUpstreamUrnCaseResolution:
    """Tests for _tables_to_urns case normalization — graph lookup with fallback."""

    @staticmethod
    def _make_source(**overrides: Any) -> "MicroStrategySource":
        base: Dict[str, Any] = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
        }
        base.update(overrides)
        config = MicroStrategyConfig.model_validate(base)
        ctx = PipelineContext(run_id="urn-case-test")
        return MicroStrategySource(config, ctx)

    def test_lowercase_fallback_when_no_graph(self) -> None:
        """Without a graph connection, convert_lineage_urns_to_lowercase=True lowercases URNs."""
        source = self._make_source(convert_lineage_urns_to_lowercase=True)
        source._warehouse_platform = "snowflake"
        urns = source._tables_to_urns(["XRBIA_DM.FACT_SALES"])
        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,xrbia_dm.fact_sales,PROD)"
        ]

    def test_original_case_when_lowercase_disabled(self) -> None:
        """convert_lineage_urns_to_lowercase=False preserves original casing."""
        source = self._make_source(convert_lineage_urns_to_lowercase=False)
        source._warehouse_platform = "snowflake"
        urns = source._tables_to_urns(["XRBIA_DM.FACT_SALES"])
        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,XRBIA_DM.FACT_SALES,PROD)"
        ]

    def test_graph_resolves_lowercase_match(self) -> None:
        """When the graph has the lowercase variant, return that URN."""
        source = self._make_source()
        source._warehouse_platform = "snowflake"

        mock_graph = MagicMock()
        # Original case doesn't exist, lowercase does
        mock_graph.exists.side_effect = lambda urn: "xrbia_dm.fact_sales" in urn
        source.ctx.graph = mock_graph

        urns = source._tables_to_urns(["XRBIA_DM.FACT_SALES"])
        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,xrbia_dm.fact_sales,PROD)"
        ]

    def test_graph_resolves_original_case_match(self) -> None:
        """When the graph has the original-case variant, return that URN."""
        source = self._make_source()
        source._warehouse_platform = "snowflake"

        mock_graph = MagicMock()
        # Original case exists
        mock_graph.exists.side_effect = lambda urn: "XRBIA_DM.FACT_SALES" in urn
        source.ctx.graph = mock_graph

        urns = source._tables_to_urns(["XRBIA_DM.FACT_SALES"])
        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,XRBIA_DM.FACT_SALES,PROD)"
        ]

    def test_graph_miss_falls_back_to_lowercase(self) -> None:
        """When graph has neither variant, fall back to convert_lineage_urns_to_lowercase."""
        source = self._make_source(convert_lineage_urns_to_lowercase=True)
        source._warehouse_platform = "snowflake"

        mock_graph = MagicMock()
        mock_graph.exists.return_value = False
        source.ctx.graph = mock_graph

        urns = source._tables_to_urns(["XRBIA_DM.FACT_SALES"])
        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,xrbia_dm.fact_sales,PROD)"
        ]

    def test_graph_error_falls_back_gracefully(self) -> None:
        """Graph API errors should not crash; fall back to config flag."""
        source = self._make_source(convert_lineage_urns_to_lowercase=True)
        source._warehouse_platform = "snowflake"

        mock_graph = MagicMock()
        mock_graph.exists.side_effect = ConnectionError("graph unavailable")
        source.ctx.graph = mock_graph

        urns = source._tables_to_urns(["XRBIA_DM.FACT_SALES"])
        assert urns == [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,xrbia_dm.fact_sales,PROD)"
        ]

    def test_already_lowercase_skips_duplicate_candidate(self) -> None:
        """When the table is already lowercase, only one candidate should be tried."""
        source = self._make_source()
        source._warehouse_platform = "snowflake"

        mock_graph = MagicMock()
        mock_graph.exists.return_value = True
        source.ctx.graph = mock_graph

        source._tables_to_urns(["xrbia_dm.fact_sales"])
        # Should only call exists once since original == lowercase
        assert mock_graph.exists.call_count == 1


class TestApiCallReduction:
    def test_cube_search_once_per_project(self) -> None:
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "P1", "status": 0},
            ]
            mock_client.get_folders.return_value = []
            mock_client.search_objects.side_effect = create_search_objects_mock(
                cubes={
                    "project_1": [
                        {"id": "cube_1", "name": "Cube", "description": ""},
                    ],
                },
            )

            source = MicroStrategySource(config, ctx)
            list(source.get_workunits())

            cube_calls = [
                c
                for c in mock_client.search_objects.call_args_list
                if (len(c.args) > 1 and c.args[1] == 776)
                or c.kwargs.get("object_type") == 776
            ]
            assert len(cube_calls) == 1

    def test_include_dashboards_false_skips_type_55_search(self) -> None:
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_dashboards": False,
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "P1", "status": 0},
            ]
            mock_client.get_folders.return_value = []
            mock_client.search_objects.side_effect = create_search_objects_mock(
                cubes={
                    "project_1": [
                        {"id": "cube_1", "name": "Cube", "description": ""},
                    ],
                },
            )

            source = MicroStrategySource(config, ctx)
            list(source.get_workunits())

            dashboard_calls = [
                c
                for c in mock_client.search_objects.call_args_list
                if (len(c.args) > 1 and c.args[1] == 55)
                or c.kwargs.get("object_type") == 55
            ]
            assert dashboard_calls == []


class TestSQLTableExtraction:
    """Tests for _extract_tables_from_sql() covering all quoting styles."""

    def test_snowflake_double_quote_schema_table(self) -> None:
        sql = 'SELECT a FROM "analytics"."fact_revenue" WHERE b = 1'
        assert _extract_tables_from_sql(sql) == ["analytics.fact_revenue"]

    def test_mysql_backtick_quoting(self) -> None:
        sql = "SELECT a FROM `mydb`.`orders` JOIN `mydb`.`customers` ON 1=1"
        result = _extract_tables_from_sql(sql)
        assert "mydb.orders" in result
        assert "mydb.customers" in result

    def test_bare_schema_dot_table(self) -> None:
        sql = "SELECT * FROM sales.revenue_by_region"
        assert _extract_tables_from_sql(sql) == ["sales.revenue_by_region"]

    def test_join_tables_collected(self) -> None:
        sql = (
            'SELECT a, b FROM "dbo"."fact_sales" '
            'JOIN "dbo"."dim_date" ON fact_sales.date_id = dim_date.id'
        )
        result = _extract_tables_from_sql(sql)
        assert "dbo.fact_sales" in result
        assert "dbo.dim_date" in result

    def test_volatile_temp_tables_skipped(self) -> None:
        # Teradata-style volatile tables: uppercase + 10+ chars starting with T
        sql = (
            "CREATE VOLATILE TABLE TD7U1ZQ9CSP000 AS "
            "(SELECT * FROM analytics.fact_revenue)"
        )
        result = _extract_tables_from_sql(sql)
        assert "analytics.fact_revenue" in result
        assert not any("TD7U1ZQ9CSP000" in t for t in result)

    def test_sql_keywords_not_captured_as_tables(self) -> None:
        sql = "SELECT a FROM schema1.table1 WHERE a > 0 GROUP BY a ORDER BY a"
        result = _extract_tables_from_sql(sql)
        assert result == ["schema1.table1"]
        for kw in ("where", "group", "order", "select"):
            assert kw not in result

    def test_empty_sql_returns_empty(self) -> None:
        assert _extract_tables_from_sql("") == []
        assert _extract_tables_from_sql("   ") == []

    def test_non_string_returns_empty(self) -> None:
        assert _extract_tables_from_sql(None) == []  # type: ignore[arg-type]

    def test_deduplicates_same_table(self) -> None:
        sql = (
            'SELECT a FROM "dbo"."orders" '
            'JOIN "dbo"."orders" o2 ON o2.id = orders.parent_id'
        )
        result = _extract_tables_from_sql(sql)
        assert result.count("dbo.orders") == 1

    def test_case_insensitive_from_join(self) -> None:
        sql = "SELECT 1 FROM schema.tbl1 INNER JOIN schema.tbl2 ON 1=1"
        result = _extract_tables_from_sql(sql)
        assert len(result) == 2


class TestWarehousePlatformDetection:
    """Tests for _detect_warehouse_platform() tier logic."""

    def _make_source(self, **extra_config: Any) -> MicroStrategySource:
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_warehouse_lineage": True,
            **extra_config,
        }
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="detect-platform-test")
        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            source = MicroStrategySource.__new__(MicroStrategySource)
            source.config = config
            source.ctx = ctx
            source.client = mock_client
            source._warehouse_platform = None
            source._detected_database = None
            source._detected_schema = None
            source.report = MagicMock()  # type: ignore[method-assign]
        return source

    def test_tier1_project_datasources_single_platform(self) -> None:
        source = self._make_source()
        source.client.get_project_datasources = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "datasourceType": "normal",
                    "database": {"type": "snow_flake"},
                    "dbms": {"name": "Snowflake"},
                    "name": "SF Prod",
                }
            ]
        )
        source._detect_warehouse_platform(project_id="proj_1")
        assert source._warehouse_platform == "snowflake"

    def test_tier1_multiple_platforms_falls_through(self) -> None:
        source = self._make_source()
        source.client.get_project_datasources = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "datasourceType": "normal",
                    "database": {"type": "snow_flake"},
                    "dbms": {"name": "Snowflake"},
                    "name": "SF",
                },
                {
                    "datasourceType": "normal",
                    "database": {"type": "mysql"},
                    "dbms": {"name": "MySQL"},
                    "name": "MySQL Dev",
                },
            ]
        )
        # Tier 1 is ambiguous; Tier 2 is empty → platform stays None
        source.client.get_datasources = MagicMock(return_value=[])  # type: ignore[method-assign]
        source.client.search_warehouse_tables = MagicMock(return_value=[])  # type: ignore[method-assign]
        source._detect_warehouse_platform(project_id="proj_1")
        assert source._warehouse_platform is None

    def test_tier2_env_datasources_used_when_tier1_empty(self) -> None:
        source = self._make_source()
        source.client.get_project_datasources = MagicMock(return_value=[])  # type: ignore[method-assign]
        source.client.get_datasources = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "datasourceType": "normal",
                    "database": {"type": "redshift"},
                    "dbms": {"name": "Redshift"},
                    "name": "RS Prod",
                }
            ]
        )
        source._detect_warehouse_platform(project_id="proj_1")
        assert source._warehouse_platform == "redshift"

    def test_already_resolved_skips_detection(self) -> None:
        source = self._make_source()
        source._warehouse_platform = "bigquery"
        source.client.get_project_datasources = MagicMock()  # type: ignore[method-assign]
        source._detect_warehouse_platform(project_id="proj_1")
        # Should NOT call any client methods since platform is already known
        source.client.get_project_datasources.assert_not_called()

    def test_tier2_env_datasources_when_project_datasources_fails(self) -> None:
        """Tier 1 raises an exception; Tier 2 env-level datasources resolves Snowflake."""
        source = self._make_source()
        source.client.get_project_datasources = MagicMock(  # type: ignore[method-assign]
            side_effect=Exception("HTTP 500")
        )
        source.client.get_datasources = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "datasourceType": "normal",
                    "database": {"type": "snow_flake"},
                    "dbms": {"name": "Snowflake"},
                    "name": "SF Prod",
                }
            ]
        )
        source._detect_warehouse_platform(project_id="proj_1")
        # Tier 1 raised — Tier 2 should still succeed
        assert source._warehouse_platform == "snowflake"

    def test_tier3_tables_api_detection(self) -> None:
        """Tiers 1–2 return empty; Tier 3a (v2/tables) resolves BigQuery."""
        source = self._make_source()
        source.client.get_project_datasources = MagicMock(return_value=[])  # type: ignore[method-assign]
        source.client.get_datasources = MagicMock(return_value=[])  # type: ignore[method-assign]
        # Tier 3: search_warehouse_tables returns one table
        source.client.search_warehouse_tables = MagicMock(  # type: ignore[method-assign]
            return_value=[{"id": "TBL001", "name": "fact_orders"}]
        )
        # get_table_definition returns a primaryDataSource reference
        source.client.get_table_definition = MagicMock(  # type: ignore[method-assign]
            return_value={"primaryDataSource": {"objectId": "DS001", "name": "BQ Prod"}}
        )
        # get_datasource_by_id returns a BigQuery datasource
        source.client.get_datasource_by_id = MagicMock(  # type: ignore[method-assign]
            return_value={
                "datasourceType": "normal",
                "database": {"type": "big_query"},
                "dbms": {"name": "BigQuery"},
                "name": "BQ Prod",
            }
        )
        source._detect_warehouse_platform(project_id="proj_1")
        assert source._warehouse_platform == "bigquery"

    def test_tier3b_model_tables_fallback(self) -> None:
        """Tier 3a (v2/tables) returns no ds_id; Tier 3b (model/tables) resolves Redshift."""
        source = self._make_source()
        source.client.get_project_datasources = MagicMock(return_value=[])  # type: ignore[method-assign]
        source.client.get_datasources = MagicMock(return_value=[])  # type: ignore[method-assign]
        # Tier 3 setup: one warehouse table
        source.client.search_warehouse_tables = MagicMock(  # type: ignore[method-assign]
            return_value=[{"id": "TBL001", "name": "fact_revenue"}]
        )
        # get_table_definition returns no primaryDataSource → triggers 3b
        source.client.get_table_definition = MagicMock(return_value={})  # type: ignore[method-assign]
        # Tier 3b: list_model_tables finds the table by name
        source.client.list_model_tables = MagicMock(  # type: ignore[method-assign]
            return_value={
                "tables": [
                    {"information": {"name": "fact_revenue", "objectId": "MTBL001"}}
                ],
                "total": 1,
            }
        )
        # get_model_table_definition returns the datasource reference
        source.client.get_model_table_definition = MagicMock(  # type: ignore[method-assign]
            return_value={"primaryDataSource": {"objectId": "DS002", "name": "RS Prod"}}
        )
        # get_datasource_by_id returns a Redshift datasource
        source.client.get_datasource_by_id = MagicMock(  # type: ignore[method-assign]
            return_value={
                "datasourceType": "normal",
                "database": {"type": "redshift"},
                "dbms": {"name": "Redshift"},
                "name": "RS Prod",
            }
        )
        source._detect_warehouse_platform(project_id="proj_1")
        assert source._warehouse_platform == "redshift"


class TestConnectionStringAutoDetection:
    """Tests for connection string parsing and auto-detection of database/schema."""

    # ── Database parsing ─────────────────────────────────────────────────────

    def test_snowflake_jdbc_url(self) -> None:
        conn = (
            "JDBC;DRIVER={net.snowflake.client.jdbc.SnowflakeDriver};"
            "URL={jdbc:snowflake://acct.snowflakecomputing.com/"
            "?warehouse=WH&db=P_MER_EDW_DB&schema=XRBIA_DM&role=MY_ROLE};"
        )
        assert _parse_database_from_connection_string(conn) == "P_MER_EDW_DB"

    def test_odbc_database_param(self) -> None:
        conn = "DRIVER={ODBC};DATABASE=MY_DB;HOSTNAME=host;PORT=5439;"
        assert _parse_database_from_connection_string(conn) == "MY_DB"

    def test_generic_catalog_param(self) -> None:
        conn = "jdbc:sqlserver://host;catalog=AdventureWorks;integratedSecurity=true"
        assert _parse_database_from_connection_string(conn) == "AdventureWorks"

    def test_no_database_returns_none(self) -> None:
        conn = "DRIVER={ODBC};SERVER=host;PORT=443;"
        assert _parse_database_from_connection_string(conn) is None

    def test_empty_string_returns_none(self) -> None:
        assert _parse_database_from_connection_string("") is None

    # ── Schema parsing ───────────────────────────────────────────────────────

    def test_snowflake_schema_from_jdbc(self) -> None:
        conn = "?db=MY_DB&schema=XRBIA_DM&role=MY_ROLE"
        assert _parse_schema_from_connection_string(conn) == "XRBIA_DM"

    def test_postgres_current_schema(self) -> None:
        conn = "DATABASE=mydb;currentSchema=analytics;"
        assert _parse_schema_from_connection_string(conn) == "analytics"

    def test_no_schema_returns_none(self) -> None:
        conn = "DATABASE=mydb;SERVER=host;"
        assert _parse_schema_from_connection_string(conn) is None

    # ── End-to-end auto-detection ────────────────────────────────────────────

    @staticmethod
    def _make_detection_source() -> "MicroStrategySource":
        config = MicroStrategyConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://demo.microstrategy.com",
                    "use_anonymous": True,
                },
                "include_warehouse_lineage": True,
            }
        )
        ctx = PipelineContext(run_id="detect-db-test")
        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            source = MicroStrategySource.__new__(MicroStrategySource)
            source.config = config
            source.ctx = ctx
            source.client = mock_client
            source._warehouse_platform = None
            source._detected_database = None
            source._detected_schema = None
            source.report = MagicMock()  # type: ignore[method-assign]
        return source

    def test_tier1_auto_detects_database_and_schema(self) -> None:
        """Tier 1 platform detection extracts both database and schema."""
        source = self._make_detection_source()
        source.client.get_project_datasources = MagicMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "datasourceType": "normal",
                    "database": {
                        "type": "snow_flake",
                        "connection": {"id": "conn123", "name": "SF_JDBC"},
                    },
                    "dbms": {"name": "Snowflake"},
                    "name": "SF Prod",
                }
            ]
        )
        source.client.get_datasource_connection = MagicMock(  # type: ignore[method-assign]
            return_value={
                "connectionString": (
                    "JDBC;URL={jdbc:snowflake://acct.snowflakecomputing.com/"
                    "?db=ANALYTICS_DB&schema=PUBLIC};"
                )
            }
        )

        source._detect_warehouse_platform(project_id="proj_1")

        assert source._warehouse_platform == "snowflake"
        assert source._detected_database == "ANALYTICS_DB"
        assert source._detected_schema == "PUBLIC"

    def test_explicit_config_overrides_auto_detection(self) -> None:
        """Config values take precedence over auto-detected values."""
        source = TestQualifyTableName._make_source(
            warehouse_lineage_database="MANUAL_DB",
            warehouse_lineage_schema="MANUAL_SCHEMA",
        )
        source._detected_database = "AUTO_DB"
        source._detected_schema = "AUTO_SCHEMA"
        assert source._qualify_table_name("SCHEMA.TABLE") == "MANUAL_DB.SCHEMA.TABLE"
        assert source._qualify_table_name("TABLE") == "MANUAL_DB.MANUAL_SCHEMA.TABLE"

    def test_auto_detected_values_used_by_qualify(self) -> None:
        """When config fields are unset, auto-detected values are used."""
        source = TestQualifyTableName._make_source()
        source._detected_database = "AUTO_DB"
        source._detected_schema = "AUTO_SCHEMA"
        assert source._qualify_table_name("SCHEMA.TABLE") == "AUTO_DB.SCHEMA.TABLE"
        assert source._qualify_table_name("TABLE") == "AUTO_DB.AUTO_SCHEMA.TABLE"


class TestIServerErrorHandling:
    """Tests for iServer error code helpers and ClassCast detection."""

    def test_is_iserver_error_matches_code(self) -> None:
        body = {"iServerCode": -2147209151, "message": "Project not loaded"}
        assert _is_iserver_error(body, -2147209151) is True

    def test_is_iserver_error_wrong_code(self) -> None:
        body = {"iServerCode": -2147072488}
        assert _is_iserver_error(body, -2147209151) is False

    def test_is_iserver_error_missing_key(self) -> None:
        body = {"message": "some error"}
        assert _is_iserver_error(body, -2147209151) is False

    def test_is_classcast_error_cannot_be_cast(self) -> None:
        body = {"message": "java.lang.Object cannot be cast to ..."}
        assert _is_classcast_error(body) is True

    def test_is_classcast_error_classcast_in_message(self) -> None:
        body = {"message": "ClassCastException in rendering"}
        assert _is_classcast_error(body) is True

    def test_is_classcast_error_unrelated_message(self) -> None:
        body = {"message": "Object not found"}
        assert _is_classcast_error(body) is False

    def test_is_classcast_error_empty_body(self) -> None:
        assert _is_classcast_error({}) is False

    def test_client_raises_project_unavailable_on_iserver_code(self) -> None:
        config = MicroStrategyConnectionConfig.model_validate(
            {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            }
        )
        client = MicroStrategyClient.__new__(MicroStrategyClient)
        client.config = config
        client.base_url = "https://demo.microstrategy.com"

        mock_response = MagicMock()
        mock_response.content = b'{"iServerCode": -2147209151, "message": "Not loaded"}'
        mock_response.json.return_value = {
            "iServerCode": -2147209151,
            "message": "Not loaded",
            "ticketId": "T1",
        }

        with pytest.raises(MicroStrategyProjectUnavailableError):
            client._raise_if_iserver_project_unavailable(
                mock_response, "/api/v2/cubes/C1"
            )


class TestReportProcessing:
    """Tests for _process_report() behavior with and without a dataSource."""

    BASE_CONFIG = {
        "connection": {
            "base_url": "https://demo.microstrategy.com",
            "use_anonymous": True,
        },
        "include_lineage": True,
        "include_ownership": True,
        "include_reports": True,
        "include_dashboards": False,
        "include_folders": False,
        "include_cubes": False,
        "include_datasets": False,
        "include_report_definitions": False,
        "include_warehouse_lineage": False,
    }

    def _run_with_reports(
        self,
        reports: List[Dict[str, Any]],
        # cube_search_results: raw cube objects as returned by search_objects (type=776).
        # _build_registries populates cube_registry from these, same as the real flow.
        cube_search_results: Optional[List[Dict[str, Any]]] = None,
        extra_config: Optional[Dict[str, Any]] = None,
    ) -> List[Any]:
        config_dict = {**self.BASE_CONFIG, **(extra_config or {})}
        config = MicroStrategyConfig.model_validate(config_dict)
        ctx = PipelineContext(run_id="report-test")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "proj_1", "name": "Proj", "status": 0},
            ]
            mock_client.get_folders.return_value = []
            mock_client.get_datasets.return_value = []
            mock_client.get_project_datasources.return_value = []
            mock_client.get_datasources.return_value = []
            mock_client.search_objects.side_effect = create_search_objects_mock(
                reports={"proj_1": reports},
                cubes={"proj_1": cube_search_results or []},
            )

            source = MicroStrategySource(config, ctx)
            return list(source.get_workunits())

    def test_report_without_datasource_emits_chart(self) -> None:
        reports = [
            {
                "id": "R001",
                "name": "Revenue Report",
                "description": "test",
                "type": 3,
                "subtype": 768,
                "owner": {"id": "USR1", "name": "Admin"},
                "dateCreated": "2023-01-01T00:00:00.000+0000",
                "dateModified": "2024-01-01T00:00:00.000+0000",
            }
        ]
        wus = self._run_with_reports(reports)
        aspect_names = [wu.metadata.aspectName for wu in wus]
        assert "chartInfo" in aspect_names

    def test_report_with_cube_datasource_emits_lineage(self) -> None:
        cube_id = "CUBE_ABC"
        reports = [
            {
                "id": "R002",
                "name": "Cube-backed Report",
                "description": "",
                "type": 3,
                "subtype": 768,
                "dataSource": {"id": cube_id},
                "owner": {"id": "USR1", "name": "Admin"},
                "dateCreated": "2023-01-01T00:00:00.000+0000",
                "dateModified": "2024-01-01T00:00:00.000+0000",
            }
        ]
        # Provide the cube via search_objects so _build_registries populates cube_registry
        cube_search = [
            {
                "id": cube_id,
                "name": "Revenue Cube",
                "description": "",
                "type": 3,
                "subtype": 776,
                "owner": {"id": "USR1", "name": "Admin"},
                "dateCreated": "2023-01-01T00:00:00.000+0000",
                "dateModified": "2024-01-01T00:00:00.000+0000",
            }
        ]
        wus = self._run_with_reports(reports, cube_search_results=cube_search)
        aspect_names = [wu.metadata.aspectName for wu in wus]
        assert "chartInfo" in aspect_names
        # Charts encode lineage in ChartInfoClass.inputs (not a separate upstreamLineage aspect)
        chart_wu = next(wu for wu in wus if wu.metadata.aspectName == "chartInfo")
        chart_inputs = chart_wu.metadata.aspect.inputs or []
        assert any(cube_id in urn for urn in chart_inputs), (
            f"Expected cube {cube_id} in chartInfo.inputs, got: {chart_inputs}"
        )

    def test_report_with_ownership_emits_ownership_aspect(self) -> None:
        reports = [
            {
                "id": "R003",
                "name": "Owned Report",
                "description": "",
                "type": 3,
                "subtype": 768,
                "owner": {"id": "USR1", "name": "Alice"},
                "dateCreated": "2023-01-01T00:00:00.000+0000",
                "dateModified": "2024-01-01T00:00:00.000+0000",
            }
        ]
        wus = self._run_with_reports(reports)
        aspect_names = [wu.metadata.aspectName for wu in wus]
        assert "ownership" in aspect_names


class TestCubeIServerErrorCodes:
    """Test that unpublished and dynamic-sourcing cubes are skipped gracefully."""

    BASE_CONFIG = {
        "connection": {
            "base_url": "https://demo.microstrategy.com",
            "use_anonymous": True,
        },
        "include_cube_schema": True,
        "include_warehouse_lineage": False,
    }

    def _make_source(self) -> MicroStrategySource:
        config = MicroStrategyConfig.model_validate(self.BASE_CONFIG)
        ctx = PipelineContext(run_id="test-iserver-codes")
        return MicroStrategySource(config, ctx)

    def test_cube_not_published_returns_none_schema(self) -> None:
        """_build_cube_schema_metadata skips cubes that report CUBE_NOT_PUBLISHED."""
        source = self._make_source()
        cube = {"id": "C001", "name": "Unpublished Cube"}
        project_id = "P001"

        source.client = MagicMock()  # type: ignore[method-assign]
        source.client.get_cube.return_value = {
            "iServerCode": ISERVER_CUBE_NOT_PUBLISHED,
            "message": "Cube not in memory",
        }

        result = source._build_cube_schema_metadata(cube, project_id)
        assert result is None

    def test_dynamic_sourcing_cube_returns_none_schema(self) -> None:
        """_build_cube_schema_metadata skips cubes that report DYNAMIC_SOURCING_CUBE."""
        source = self._make_source()
        cube = {"id": "C002", "name": "Dynamic Cube"}
        project_id = "P001"

        source.client = MagicMock()  # type: ignore[method-assign]
        source.client.get_cube.return_value = {
            "iServerCode": ISERVER_DYNAMIC_SOURCING_CUBE,
            "message": "Attribute form cache cube",
        }

        result = source._build_cube_schema_metadata(cube, project_id)
        assert result is None


class TestLibraryDatasetIngestion:
    """Test that library datasets are emitted as SDK V2 Dataset entities."""

    BASE_CONFIG = {
        "connection": {
            "base_url": "https://demo.microstrategy.com",
            "use_anonymous": True,
        },
        "include_lineage": False,
        "include_ownership": True,
        "include_cubes": False,
        "include_dashboards": False,
        "include_reports": False,
        "include_folders": False,
        "include_datasets": True,
    }

    def test_library_dataset_emits_dataset_entity(self) -> None:
        """_process_dataset emits a Dataset entity whose URN encodes project+dataset IDs."""
        config = MicroStrategyConfig.model_validate(self.BASE_CONFIG)
        ctx = PipelineContext(run_id="test-library-dataset")
        source = MicroStrategySource(config, ctx)

        project = {"id": "PROJ001", "name": "My Project"}
        dataset = {
            "id": "DS001",
            "name": "Customer Dataset",
            "description": "Customer dimension table",
            "type": 3,
            "owner": {"id": "USR1", "name": "Alice"},
        }

        entities = list(source._process_dataset(dataset, project))
        assert len(entities) == 1
        entity = entities[0]
        # URN encodes project.id + dataset.id
        assert "PROJ001" in str(entity.urn)  # type: ignore[union-attr]
        assert "DS001" in str(entity.urn)  # type: ignore[union-attr]

    def test_library_dataset_includes_ownership_when_enabled(self) -> None:
        """_process_dataset emits an ownership aspect when include_ownership=True."""
        config = MicroStrategyConfig.model_validate(self.BASE_CONFIG)
        ctx = PipelineContext(run_id="test-library-dataset-ownership")
        source = MicroStrategySource(config, ctx)

        project = {"id": "PROJ001", "name": "My Project"}
        dataset = {
            "id": "DS002",
            "name": "Sales Dataset",
            "type": 3,
            "owner": {"id": "USR2", "name": "Bob"},
        }

        entities = list(source._process_dataset(dataset, project))
        # Expand SDK V2 entity into individual aspect work units to check ownership
        wus = [wu for entity in entities for wu in entity.as_workunits()]  # type: ignore[union-attr]
        aspect_names = [wu.metadata.aspectName for wu in wus]  # type: ignore[union-attr]
        assert "ownership" in aspect_names


# ── Pattern filtering tests ────────────────────────────────────────────────────


class TestReportPatternFiltering:
    """report_pattern allow/deny controls which reports are processed."""

    def _run(self, reports: List[Dict[str, Any]], pattern: Dict) -> List[Any]:
        config = MicroStrategyConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://demo.microstrategy.com",
                    "use_anonymous": True,
                },
                "include_folders": False,
                "include_dashboards": False,
                "include_cubes": False,
                "include_datasets": False,
                "include_lineage": False,
                "report_pattern": pattern,
            }
        )
        ctx = PipelineContext(run_id="report-pattern-test")
        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get_projects.return_value = [
                {"id": "P1", "name": "Proj", "status": 0}
            ]
            mock_client.get_folders.return_value = []
            mock_client.get_datasets.return_value = []
            mock_client.search_objects.side_effect = create_search_objects_mock(
                reports={"P1": reports}
            )
            source = MicroStrategySource(config, ctx)
            return list(source.get_workunits())

    def test_report_pattern_allow_filters_by_name(self) -> None:
        """Only reports matching the allow pattern should emit chartInfo."""
        reports = [
            {
                "id": "R1",
                "name": "Revenue Report",
                "description": "",
                "type": 3,
                "subtype": 768,
            },
            {
                "id": "R2",
                "name": "Cost Report",
                "description": "",
                "type": 3,
                "subtype": 768,
            },
            {
                "id": "R3",
                "name": "Revenue Summary",
                "description": "",
                "type": 3,
                "subtype": 768,
            },
        ]
        wus = self._run(reports, {"allow": ["^Revenue.*"]})
        chart_urns = {
            wu.metadata.entityUrn
            for wu in wus
            if hasattr(wu.metadata, "aspectName")
            and wu.metadata.aspectName == "chartInfo"
        }
        # R1 and R3 match the allow pattern; R2 should be excluded
        assert any("R1" in u for u in chart_urns)
        assert any("R3" in u for u in chart_urns)
        assert not any("R2" in u for u in chart_urns)

    def test_report_pattern_deny_excludes_by_name(self) -> None:
        """Reports matching the deny pattern should be skipped."""
        reports = [
            {
                "id": "R1",
                "name": "Draft Revenue",
                "description": "",
                "type": 3,
                "subtype": 768,
            },
            {
                "id": "R2",
                "name": "Published Revenue",
                "description": "",
                "type": 3,
                "subtype": 768,
            },
        ]
        wus = self._run(reports, {"deny": ["^Draft.*"]})
        chart_urns = {
            wu.metadata.entityUrn
            for wu in wus
            if hasattr(wu.metadata, "aspectName")
            and wu.metadata.aspectName == "chartInfo"
        }
        assert any("R2" in u for u in chart_urns)
        assert not any("R1" in u for u in chart_urns)


class TestCubePatternFiltering:
    """cube_pattern allow/deny controls which cubes are processed."""

    def _run(self, cubes: List[Dict[str, Any]], pattern: Dict) -> List[Any]:
        config = MicroStrategyConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://demo.microstrategy.com",
                    "use_anonymous": True,
                },
                "include_folders": False,
                "include_dashboards": False,
                "include_reports": False,
                "include_datasets": False,
                "include_lineage": False,
                "include_cubes": True,
                "cube_pattern": pattern,
            }
        )
        ctx = PipelineContext(run_id="cube-pattern-test")
        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get_projects.return_value = [
                {"id": "P1", "name": "Proj", "status": 0}
            ]
            mock_client.get_folders.return_value = []
            mock_client.get_datasets.return_value = []
            mock_client.get_cube_sql_view.return_value = ""
            mock_client.search_objects.side_effect = create_search_objects_mock(
                cubes={"P1": cubes}
            )
            source = MicroStrategySource(config, ctx)
            return list(source.get_workunits())

    def test_cube_pattern_allow_filters_by_name(self) -> None:
        """Only cubes matching the allow pattern should emit datasetProperties."""
        cubes = [
            {"id": "C1", "name": "Revenue Cube", "description": "", "subtype": 776},
            {"id": "C2", "name": "Cost Cube", "description": "", "subtype": 776},
        ]
        wus = self._run(cubes, {"allow": ["^Revenue.*"]})
        dataset_prop_urns = {
            wu.metadata.entityUrn
            for wu in wus
            if hasattr(wu.metadata, "aspectName")
            and wu.metadata.aspectName == "datasetProperties"
        }
        assert any("C1" in u for u in dataset_prop_urns)
        assert not any("C2" in u for u in dataset_prop_urns)

    def test_cube_pattern_deny_excludes_by_name(self) -> None:
        """Cubes matching the deny pattern should be skipped."""
        cubes = [
            {"id": "C1", "name": "Prod Cube", "description": "", "subtype": 776},
            {"id": "C2", "name": "Test Cube", "description": "", "subtype": 776},
        ]
        wus = self._run(cubes, {"deny": [".*Test.*"]})
        dataset_prop_urns = {
            wu.metadata.entityUrn
            for wu in wus
            if hasattr(wu.metadata, "aspectName")
            and wu.metadata.aspectName == "datasetProperties"
        }
        assert any("C1" in u for u in dataset_prop_urns)
        assert not any("C2" in u for u in dataset_prop_urns)


class TestFolderPatternFiltering:
    """folder_pattern allow/deny controls which folders are emitted as containers."""

    def _run(self, folders: List[Dict[str, Any]], pattern: Dict) -> List[Any]:
        config = MicroStrategyConfig.model_validate(
            {
                "connection": {
                    "base_url": "https://demo.microstrategy.com",
                    "use_anonymous": True,
                },
                "include_folders": True,
                "include_dashboards": False,
                "include_reports": False,
                "include_cubes": False,
                "include_datasets": False,
                "include_lineage": False,
                "folder_pattern": pattern,
            }
        )
        ctx = PipelineContext(run_id="folder-pattern-test")
        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get_projects.return_value = [
                {"id": "P1", "name": "Proj", "status": 0}
            ]
            mock_client.get_folders.return_value = folders
            mock_client.get_datasets.return_value = []
            mock_client.search_objects.side_effect = create_search_objects_mock()
            source = MicroStrategySource(config, ctx)
            return list(source.get_workunits())

    def test_folder_pattern_allow_emits_only_matching_containers(self) -> None:
        """Folders not matching the allow pattern should not produce container aspects."""
        folders = [
            {"id": "F1", "name": "Public Reports", "type": 8, "subtype": 8192},
            {"id": "F2", "name": "Private Reports", "type": 8, "subtype": 8192},
        ]
        wus = self._run(folders, {"allow": ["^Public.*"]})
        container_aspects = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspectName")
            and wu.metadata.aspectName == "containerProperties"
        ]
        container_display_names = {
            wu.metadata.aspect.name  # type: ignore[union-attr]
            for wu in container_aspects
            if hasattr(wu.metadata, "aspect")
        }
        assert "Public Reports" in container_display_names
        assert "Private Reports" not in container_display_names

    def test_folder_pattern_deny_excludes_matching_folders(self) -> None:
        """Folders matching the deny pattern should not produce container aspects."""
        folders = [
            {"id": "F1", "name": "Shared Reports", "type": 8, "subtype": 8192},
            {"id": "F2", "name": "Archive Reports", "type": 8, "subtype": 8192},
        ]
        wus = self._run(folders, {"deny": ["^Archive.*"]})
        container_aspects = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspectName")
            and wu.metadata.aspectName == "containerProperties"
        ]
        container_display_names = {
            wu.metadata.aspect.name  # type: ignore[union-attr]
            for wu in container_aspects
            if hasattr(wu.metadata, "aspect")
        }
        assert "Shared Reports" in container_display_names
        assert "Archive Reports" not in container_display_names
