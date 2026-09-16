"""Complex mock data for Azure Data Factory integration tests.

This module provides mock data for testing complex ADF pipeline patterns:
1. Nested Pipelines (Execute Pipeline activity)
2. ForEach Loops with multiple activities
3. Control Flow Branching (If-Condition, Switch)
4. Mapping Data Flows with transformations
5. Multi-Source Copy Pipelines (SQL → Blob → Synapse)
"""

from typing import Any, Dict, List

# Common test constants
SUBSCRIPTION_ID = "12345678-1234-1234-1234-123456789012"
RESOURCE_GROUP = "complex-test-rg"
FACTORY_NAME = "complex-data-factory"
LOCATION = "eastus"


def _base_resource_id(resource_type: str, name: str) -> str:
    """Generate a standard Azure resource ID."""
    return (
        f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}"
        f"/providers/Microsoft.DataFactory/factories/{FACTORY_NAME}/{resource_type}/{name}"
    )


# =============================================================================
# LINKED SERVICES - Various platform types for lineage testing
# =============================================================================


def create_complex_linked_services() -> List[Dict[str, Any]]:
    """Create linked services for multiple platforms."""
    return [
        {
            "id": _base_resource_id("linkedservices", "SqlServerSource"),
            "name": "SqlServerSource",
            "type": "Microsoft.DataFactory/factories/linkedservices",
            "properties": {
                "type": "AzureSqlDatabase",
                "typeProperties": {
                    "connectionString": "Server=sql-server.database.windows.net;Database=SourceDB"
                },
            },
        },
        {
            "id": _base_resource_id("linkedservices", "BlobStorage"),
            "name": "BlobStorage",
            "type": "Microsoft.DataFactory/factories/linkedservices",
            "properties": {
                "type": "AzureBlobStorage",
                "typeProperties": {
                    "connectionString": "DefaultEndpointsProtocol=https"
                },
            },
        },
        {
            "id": _base_resource_id("linkedservices", "SynapseDestination"),
            "name": "SynapseDestination",
            "type": "Microsoft.DataFactory/factories/linkedservices",
            "properties": {
                "type": "AzureSynapseAnalytics",
                "typeProperties": {
                    "connectionString": "Server=synapse.sql.azuresynapse.net;Database=DW"
                },
            },
        },
        {
            "id": _base_resource_id("linkedservices", "SnowflakeConnection"),
            "name": "SnowflakeConnection",
            "type": "Microsoft.DataFactory/factories/linkedservices",
            "properties": {
                "type": "Snowflake",
                "typeProperties": {"connectionString": "account=myaccount"},
            },
        },
        {
            "id": _base_resource_id("linkedservices", "DataLakeStorage"),
            "name": "DataLakeStorage",
            "type": "Microsoft.DataFactory/factories/linkedservices",
            "properties": {
                "type": "AzureBlobFS",
                "typeProperties": {"url": "https://datalake.dfs.core.windows.net"},
            },
        },
    ]


# =============================================================================
# DATASETS - Input/output datasets for lineage
# =============================================================================


def create_complex_datasets() -> List[Dict[str, Any]]:
    """Create datasets for complex lineage scenarios."""
    return [
        # SQL Server datasets
        {
            "id": _base_resource_id("datasets", "SqlCustomersTable"),
            "name": "SqlCustomersTable",
            "type": "Microsoft.DataFactory/factories/datasets",
            "properties": {
                "linkedServiceName": {
                    "referenceName": "SqlServerSource",
                    "type": "LinkedServiceReference",
                },
                "type": "AzureSqlTable",
                "typeProperties": {"schema": "dbo", "table": "Customers"},
            },
        },
        {
            "id": _base_resource_id("datasets", "SqlOrdersTable"),
            "name": "SqlOrdersTable",
            "type": "Microsoft.DataFactory/factories/datasets",
            "properties": {
                "linkedServiceName": {
                    "referenceName": "SqlServerSource",
                    "type": "LinkedServiceReference",
                },
                "type": "AzureSqlTable",
                "typeProperties": {"schema": "dbo", "table": "Orders"},
            },
        },
        {
            "id": _base_resource_id("datasets", "SqlProductsTable"),
            "name": "SqlProductsTable",
            "type": "Microsoft.DataFactory/factories/datasets",
            "properties": {
                "linkedServiceName": {
                    "referenceName": "SqlServerSource",
                    "type": "LinkedServiceReference",
                },
                "type": "AzureSqlTable",
                "typeProperties": {"schema": "dbo", "table": "Products"},
            },
        },
        # Blob storage datasets
        {
            "id": _base_resource_id("datasets", "BlobStagingCustomers"),
            "name": "BlobStagingCustomers",
            "type": "Microsoft.DataFactory/factories/datasets",
            "properties": {
                "linkedServiceName": {
                    "referenceName": "BlobStorage",
                    "type": "LinkedServiceReference",
                },
                "type": "DelimitedText",
                "typeProperties": {
                    "location": {
                        "type": "AzureBlobStorageLocation",
                        "container": "staging",
                        "folderPath": "customers",
                    }
                },
            },
        },
        {
            "id": _base_resource_id("datasets", "BlobStagingOrders"),
            "name": "BlobStagingOrders",
            "type": "Microsoft.DataFactory/factories/datasets",
            "properties": {
                "linkedServiceName": {
                    "referenceName": "BlobStorage",
                    "type": "LinkedServiceReference",
                },
                "type": "DelimitedText",
                "typeProperties": {
                    "location": {
                        "type": "AzureBlobStorageLocation",
                        "container": "staging",
                        "folderPath": "orders",
                    }
                },
            },
        },
        # Synapse datasets
        {
            "id": _base_resource_id("datasets", "SynapseCustomersDim"),
            "name": "SynapseCustomersDim",
            "type": "Microsoft.DataFactory/factories/datasets",
            "properties": {
                "linkedServiceName": {
                    "referenceName": "SynapseDestination",
                    "type": "LinkedServiceReference",
                },
                "type": "AzureSqlDWTable",
                "typeProperties": {"schema": "dw", "table": "DimCustomers"},
            },
        },
        {
            "id": _base_resource_id("datasets", "SynapseOrdersFact"),
            "name": "SynapseOrdersFact",
            "type": "Microsoft.DataFactory/factories/datasets",
            "properties": {
                "linkedServiceName": {
                    "referenceName": "SynapseDestination",
                    "type": "LinkedServiceReference",
                },
                "type": "AzureSqlDWTable",
                "typeProperties": {"schema": "dw", "table": "FactOrders"},
            },
        },
        # Data Lake datasets for Data Flow
        {
            "id": _base_resource_id("datasets", "DataLakeRawData"),
            "name": "DataLakeRawData",
            "type": "Microsoft.DataFactory/factories/datasets",
            "properties": {
                "linkedServiceName": {
                    "referenceName": "DataLakeStorage",
                    "type": "LinkedServiceReference",
                },
                "type": "Parquet",
                "typeProperties": {
                    "location": {
                        "type": "AzureBlobFSLocation",
                        "fileSystem": "raw",
                        "folderPath": "sales",
                    }
                },
            },
        },
        {
            "id": _base_resource_id("datasets", "DataLakeCuratedData"),
            "name": "DataLakeCuratedData",
            "type": "Microsoft.DataFactory/factories/datasets",
            "properties": {
                "linkedServiceName": {
                    "referenceName": "DataLakeStorage",
                    "type": "LinkedServiceReference",
                },
                "type": "Parquet",
                "typeProperties": {
                    "location": {
                        "type": "AzureBlobFSLocation",
                        "fileSystem": "curated",
                        "folderPath": "sales_summary",
                    }
                },
            },
        },
    ]


# =============================================================================
# SCENARIO 1: NESTED PIPELINES (Execute Pipeline Activity)
# =============================================================================


def create_nested_pipeline_scenario() -> Dict[str, Any]:
    """Create mock data for nested pipeline scenario.

    Structure:
    - ParentOrchestrationPipeline
      └── ExecutePipeline: ChildDataMovementPipeline
          └── Copy: SqlToBlob
      └── ExecutePipeline: ChildTransformPipeline
          └── DataFlow: TransformData
    """
    child_data_movement = {
        "id": _base_resource_id("pipelines", "ChildDataMovementPipeline"),
        "name": "ChildDataMovementPipeline",
        "type": "Microsoft.DataFactory/factories/pipelines",
        "properties": {
            "description": "Child pipeline for data movement",
            "activities": [
                {
                    "name": "CopyCustomersToStaging",
                    "type": "Copy",
                    "inputs": [
                        {
                            "referenceName": "SqlCustomersTable",
                            "type": "DatasetReference",
                        }
                    ],
                    "outputs": [
                        {
                            "referenceName": "BlobStagingCustomers",
                            "type": "DatasetReference",
                        }
                    ],
                    "typeProperties": {
                        "source": {"type": "AzureSqlSource"},
                        "sink": {"type": "DelimitedTextSink"},
                    },
                }
            ],
            "parameters": {"sourceTable": {"type": "String"}},
        },
    }

    child_transform = {
        "id": _base_resource_id("pipelines", "ChildTransformPipeline"),
        "name": "ChildTransformPipeline",
        "type": "Microsoft.DataFactory/factories/pipelines",
        "properties": {
            "description": "Child pipeline for data transformation",
            "activities": [
                {
                    "name": "TransformCustomerData",
                    "type": "ExecuteDataFlow",
                    "typeProperties": {
                        "dataflow": {
                            "referenceName": "CustomerTransformFlow",
                            "type": "DataFlowReference",
                        }
                    },
                }
            ],
        },
    }

    parent_pipeline = {
        "id": _base_resource_id("pipelines", "ParentOrchestrationPipeline"),
        "name": "ParentOrchestrationPipeline",
        "type": "Microsoft.DataFactory/factories/pipelines",
        "properties": {
            "description": "Parent orchestration pipeline that calls child pipelines",
            "activities": [
                {
                    "name": "ExecuteDataMovement",
                    "type": "ExecutePipeline",
                    "typeProperties": {
                        "pipeline": {
                            "referenceName": "ChildDataMovementPipeline",
                            "type": "PipelineReference",
                        },
                        "waitOnCompletion": True,
                        "parameters": {"sourceTable": "Customers"},
                    },
                },
                {
                    "name": "ExecuteTransform",
                    "type": "ExecutePipeline",
                    "dependsOn": [
                        {
                            "activity": "ExecuteDataMovement",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "typeProperties": {
                        "pipeline": {
                            "referenceName": "ChildTransformPipeline",
                            "type": "PipelineReference",
                        },
                        "waitOnCompletion": True,
                    },
                },
            ],
        },
    }

    return {
        "pipelines": [parent_pipeline, child_data_movement, child_transform],
        "expected_dataflows": 3,
        "expected_datajobs": 4,  # 2 ExecutePipeline + 1 Copy + 1 DataFlow
    }


# =============================================================================
# SCENARIO 2: FOREACH LOOPS
# =============================================================================


def create_foreach_loop_scenario() -> Dict[str, Any]:
    """Create mock data for ForEach loop scenario.

    Structure:
    - ForEachTablePipeline
      └── ForEach: IterateOverTables
          └── Copy: CopyTableToStaging (parametrized)
    """
    pipeline = {
        "id": _base_resource_id("pipelines", "ForEachTablePipeline"),
        "name": "ForEachTablePipeline",
        "type": "Microsoft.DataFactory/factories/pipelines",
        "properties": {
            "description": "Pipeline with ForEach loop to copy multiple tables",
            "parameters": {
                "tableList": {
                    "type": "Array",
                    "defaultValue": ["Customers", "Orders", "Products"],
                }
            },
            "activities": [
                {
                    "name": "GetTableList",
                    "type": "Lookup",
                    "typeProperties": {
                        "source": {
                            "type": "AzureSqlSource",
                            "sqlReaderQuery": "SELECT name FROM sys.tables",
                        },
                        "dataset": {
                            "referenceName": "SqlCustomersTable",
                            "type": "DatasetReference",
                        },
                        "firstRowOnly": False,
                    },
                },
                {
                    "name": "IterateOverTables",
                    "type": "ForEach",
                    "dependsOn": [
                        {
                            "activity": "GetTableList",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "typeProperties": {
                        "items": {
                            "value": "@activity('GetTableList').output.value",
                            "type": "Expression",
                        },
                        "isSequential": False,
                        "batchCount": 5,
                        "activities": [
                            {
                                "name": "CopyTableToStaging",
                                "type": "Copy",
                                "inputs": [
                                    {
                                        "referenceName": "SqlCustomersTable",
                                        "type": "DatasetReference",
                                    }
                                ],
                                "outputs": [
                                    {
                                        "referenceName": "BlobStagingCustomers",
                                        "type": "DatasetReference",
                                    }
                                ],
                                "typeProperties": {
                                    "source": {"type": "AzureSqlSource"},
                                    "sink": {"type": "DelimitedTextSink"},
                                },
                            }
                        ],
                    },
                },
            ],
        },
    }

    return {
        "pipelines": [pipeline],
        "expected_dataflows": 1,
        "expected_datajobs": 3,  # Lookup + ForEach + Copy (inside ForEach)
    }


# =============================================================================
# SCENARIO 3: CONTROL FLOW BRANCHING (If-Condition, Switch)
# =============================================================================


def create_branching_scenario() -> Dict[str, Any]:
    """Create mock data for control flow branching scenario.

    Structure:
    - BranchingPipeline
      └── Lookup: CheckDataExists
      └── IfCondition: DataExistsCheck
          ├── True: Copy: FullLoad
          └── False: Copy: IncrementalLoad
      └── Switch: ProcessByRegion
          ├── Case "US": Copy: ProcessUSData
          ├── Case "EU": Copy: ProcessEUData
          └── Default: Copy: ProcessOtherData
    """
    pipeline = {
        "id": _base_resource_id("pipelines", "BranchingPipeline"),
        "name": "BranchingPipeline",
        "type": "Microsoft.DataFactory/factories/pipelines",
        "properties": {
            "description": "Pipeline with If-Condition and Switch branching",
            "parameters": {"region": {"type": "String", "defaultValue": "US"}},
            "activities": [
                {
                    "name": "CheckDataExists",
                    "type": "Lookup",
                    "typeProperties": {
                        "source": {
                            "type": "AzureSqlSource",
                            "sqlReaderQuery": "SELECT COUNT(*) as cnt FROM dbo.Customers",
                        },
                        "dataset": {
                            "referenceName": "SqlCustomersTable",
                            "type": "DatasetReference",
                        },
                    },
                },
                {
                    "name": "DataExistsCheck",
                    "type": "IfCondition",
                    "dependsOn": [
                        {
                            "activity": "CheckDataExists",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "typeProperties": {
                        "expression": {
                            "value": "@greater(activity('CheckDataExists').output.firstRow.cnt, 0)",
                            "type": "Expression",
                        },
                        "ifTrueActivities": [
                            {
                                "name": "FullLoad",
                                "type": "Copy",
                                "inputs": [
                                    {
                                        "referenceName": "SqlCustomersTable",
                                        "type": "DatasetReference",
                                    }
                                ],
                                "outputs": [
                                    {
                                        "referenceName": "BlobStagingCustomers",
                                        "type": "DatasetReference",
                                    }
                                ],
                                "typeProperties": {
                                    "source": {"type": "AzureSqlSource"},
                                    "sink": {"type": "DelimitedTextSink"},
                                },
                            }
                        ],
                        "ifFalseActivities": [
                            {
                                "name": "IncrementalLoad",
                                "type": "Copy",
                                "inputs": [
                                    {
                                        "referenceName": "SqlOrdersTable",
                                        "type": "DatasetReference",
                                    }
                                ],
                                "outputs": [
                                    {
                                        "referenceName": "BlobStagingOrders",
                                        "type": "DatasetReference",
                                    }
                                ],
                                "typeProperties": {
                                    "source": {"type": "AzureSqlSource"},
                                    "sink": {"type": "DelimitedTextSink"},
                                },
                            }
                        ],
                    },
                },
                {
                    "name": "ProcessByRegion",
                    "type": "Switch",
                    "dependsOn": [
                        {
                            "activity": "DataExistsCheck",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "typeProperties": {
                        "on": {
                            "value": "@pipeline().parameters.region",
                            "type": "Expression",
                        },
                        "cases": [
                            {
                                "value": "US",
                                "activities": [
                                    {
                                        "name": "ProcessUSData",
                                        "type": "Copy",
                                        "inputs": [
                                            {
                                                "referenceName": "SqlCustomersTable",
                                                "type": "DatasetReference",
                                            }
                                        ],
                                        "outputs": [
                                            {
                                                "referenceName": "SynapseCustomersDim",
                                                "type": "DatasetReference",
                                            }
                                        ],
                                        "typeProperties": {
                                            "source": {"type": "AzureSqlSource"},
                                            "sink": {"type": "SqlDWSink"},
                                        },
                                    }
                                ],
                            },
                            {
                                "value": "EU",
                                "activities": [
                                    {
                                        "name": "ProcessEUData",
                                        "type": "Copy",
                                        "inputs": [
                                            {
                                                "referenceName": "SqlOrdersTable",
                                                "type": "DatasetReference",
                                            }
                                        ],
                                        "outputs": [
                                            {
                                                "referenceName": "SynapseOrdersFact",
                                                "type": "DatasetReference",
                                            }
                                        ],
                                        "typeProperties": {
                                            "source": {"type": "AzureSqlSource"},
                                            "sink": {"type": "SqlDWSink"},
                                        },
                                    }
                                ],
                            },
                        ],
                        "defaultActivities": [
                            {
                                "name": "ProcessOtherData",
                                "type": "Copy",
                                "inputs": [
                                    {
                                        "referenceName": "SqlProductsTable",
                                        "type": "DatasetReference",
                                    }
                                ],
                                "outputs": [
                                    {
                                        "referenceName": "BlobStagingCustomers",
                                        "type": "DatasetReference",
                                    }
                                ],
                                "typeProperties": {
                                    "source": {"type": "AzureSqlSource"},
                                    "sink": {"type": "DelimitedTextSink"},
                                },
                            }
                        ],
                    },
                },
            ],
        },
    }

    return {
        "pipelines": [pipeline],
        "expected_dataflows": 1,
        "expected_datajobs": 8,  # Lookup + IfCondition + 2 Copy (if branches) + Switch + 3 Copy (switch cases)
    }


# =============================================================================
# SCENARIO 4: MAPPING DATA FLOWS
# =============================================================================


def create_dataflow_scenario() -> Dict[str, Any]:
    """Create mock data for Mapping Data Flow scenario.

    Structure:
    - DataFlowPipeline
      └── ExecuteDataFlow: RunSalesTransformation
          └── SalesTransformationFlow (sources → transforms → sinks)
    """
    data_flow = {
        "id": _base_resource_id("dataflows", "SalesTransformationFlow"),
        "name": "SalesTransformationFlow",
        "type": "Microsoft.DataFactory/factories/dataflows",
        "properties": {
            "type": "MappingDataFlow",
            "description": "Complex data flow with multiple sources and transformations",
            "typeProperties": {
                "sources": [
                    {
                        "name": "CustomersSource",
                        "dataset": {
                            "referenceName": "DataLakeRawData",
                            "type": "DatasetReference",
                        },
                    },
                    {
                        "name": "OrdersSource",
                        "dataset": {
                            "referenceName": "SqlOrdersTable",
                            "type": "DatasetReference",
                        },
                    },
                ],
                "sinks": [
                    {
                        "name": "CuratedOutput",
                        "dataset": {
                            "referenceName": "DataLakeCuratedData",
                            "type": "DatasetReference",
                        },
                    },
                    {
                        "name": "SynapseOutput",
                        "dataset": {
                            "referenceName": "SynapseCustomersDim",
                            "type": "DatasetReference",
                        },
                    },
                ],
                "transformations": [
                    {
                        "name": "FilterActiveCustomers",
                        "description": "Filter only active customers",
                    },
                    {
                        "name": "JoinOrdersToCustomers",
                        "description": "Join orders with customers",
                    },
                    {
                        "name": "AggregateByRegion",
                        "description": "Aggregate sales by region",
                    },
                    {
                        "name": "DeriveMetrics",
                        "description": "Calculate derived metrics",
                    },
                ],
                "scriptLines": [
                    "source(output(",
                    "    customer_id as integer,",
                    "    name as string,",
                    "    region as string,",
                    "    is_active as boolean",
                    "  ),",
                    "  allowSchemaDrift: true) ~> CustomersSource",
                    "source(output(",
                    "    order_id as integer,",
                    "    customer_id as integer,",
                    "    amount as decimal(10,2)",
                    "  )) ~> OrdersSource",
                    "CustomersSource filter(is_active == true()) ~> FilterActiveCustomers",
                    "FilterActiveCustomers, OrdersSource join(",
                    "    CustomersSource.customer_id == OrdersSource.customer_id",
                    "  ) ~> JoinOrdersToCustomers",
                    "JoinOrdersToCustomers aggregate(",
                    "    groupBy(region),",
                    "    total_sales = sum(amount)",
                    "  ) ~> AggregateByRegion",
                    "AggregateByRegion derive(",
                    "    avg_order = total_sales / count(order_id)",
                    "  ) ~> DeriveMetrics",
                    "DeriveMetrics sink() ~> CuratedOutput",
                    "DeriveMetrics sink() ~> SynapseOutput",
                ],
            },
        },
    }

    pipeline = {
        "id": _base_resource_id("pipelines", "DataFlowPipeline"),
        "name": "DataFlowPipeline",
        "type": "Microsoft.DataFactory/factories/pipelines",
        "properties": {
            "description": "Pipeline that executes a mapping data flow",
            "activities": [
                {
                    "name": "RunSalesTransformation",
                    "type": "ExecuteDataFlow",
                    "typeProperties": {
                        "dataflow": {
                            "referenceName": "SalesTransformationFlow",
                            "type": "DataFlowReference",
                        },
                        "compute": {"coreCount": 8, "computeType": "General"},
                    },
                }
            ],
        },
    }

    return {
        "pipelines": [pipeline],
        "data_flows": [data_flow],
        "expected_dataflows": 1,
        "expected_datajobs": 1,
        "expected_lineage_sources": 2,  # DataLakeRawData, SqlOrdersTable
        "expected_lineage_sinks": 2,  # DataLakeCuratedData, SynapseCustomersDim
    }


# =============================================================================
# SCENARIO 5: MULTI-SOURCE COPY CHAIN (SQL → Blob → Synapse)
# =============================================================================


def create_multisource_chain_scenario() -> Dict[str, Any]:
    """Create mock data for multi-source copy chain scenario.

    Structure:
    - ETLPipeline
      └── Copy: ExtractFromSQL (SQL → Blob)
      └── Copy: LoadToSynapse (Blob → Synapse)
      └── Copy: ArchiveToDataLake (Blob → DataLake)

    This tests end-to-end lineage: SQL → Blob → Synapse
                                        └─→ DataLake
    """
    pipeline = {
        "id": _base_resource_id("pipelines", "ETLPipeline"),
        "name": "ETLPipeline",
        "type": "Microsoft.DataFactory/factories/pipelines",
        "properties": {
            "description": "Full ETL pipeline: Extract from SQL, stage in Blob, load to Synapse and archive",
            "activities": [
                {
                    "name": "ExtractCustomersFromSQL",
                    "type": "Copy",
                    "inputs": [
                        {
                            "referenceName": "SqlCustomersTable",
                            "type": "DatasetReference",
                        }
                    ],
                    "outputs": [
                        {
                            "referenceName": "BlobStagingCustomers",
                            "type": "DatasetReference",
                        }
                    ],
                    "typeProperties": {
                        "source": {"type": "AzureSqlSource"},
                        "sink": {"type": "DelimitedTextSink"},
                    },
                },
                {
                    "name": "ExtractOrdersFromSQL",
                    "type": "Copy",
                    "inputs": [
                        {"referenceName": "SqlOrdersTable", "type": "DatasetReference"}
                    ],
                    "outputs": [
                        {
                            "referenceName": "BlobStagingOrders",
                            "type": "DatasetReference",
                        }
                    ],
                    "typeProperties": {
                        "source": {"type": "AzureSqlSource"},
                        "sink": {"type": "DelimitedTextSink"},
                    },
                },
                {
                    "name": "LoadCustomersToSynapse",
                    "type": "Copy",
                    "dependsOn": [
                        {
                            "activity": "ExtractCustomersFromSQL",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "inputs": [
                        {
                            "referenceName": "BlobStagingCustomers",
                            "type": "DatasetReference",
                        }
                    ],
                    "outputs": [
                        {
                            "referenceName": "SynapseCustomersDim",
                            "type": "DatasetReference",
                        }
                    ],
                    "typeProperties": {
                        "source": {"type": "DelimitedTextSource"},
                        "sink": {"type": "SqlDWSink", "allowPolyBase": True},
                    },
                },
                {
                    "name": "LoadOrdersToSynapse",
                    "type": "Copy",
                    "dependsOn": [
                        {
                            "activity": "ExtractOrdersFromSQL",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "inputs": [
                        {
                            "referenceName": "BlobStagingOrders",
                            "type": "DatasetReference",
                        }
                    ],
                    "outputs": [
                        {
                            "referenceName": "SynapseOrdersFact",
                            "type": "DatasetReference",
                        }
                    ],
                    "typeProperties": {
                        "source": {"type": "DelimitedTextSource"},
                        "sink": {"type": "SqlDWSink", "allowPolyBase": True},
                    },
                },
                {
                    "name": "ArchiveToDataLake",
                    "type": "Copy",
                    "dependsOn": [
                        {
                            "activity": "LoadCustomersToSynapse",
                            "dependencyConditions": ["Succeeded"],
                        },
                        {
                            "activity": "LoadOrdersToSynapse",
                            "dependencyConditions": ["Succeeded"],
                        },
                    ],
                    "inputs": [
                        {
                            "referenceName": "BlobStagingCustomers",
                            "type": "DatasetReference",
                        }
                    ],
                    "outputs": [
                        {"referenceName": "DataLakeRawData", "type": "DatasetReference"}
                    ],
                    "typeProperties": {
                        "source": {"type": "DelimitedTextSource"},
                        "sink": {"type": "ParquetSink"},
                    },
                },
            ],
        },
    }

    return {
        "pipelines": [pipeline],
        "expected_dataflows": 1,
        "expected_datajobs": 5,
        "expected_lineage_edges": [
            # Stage 1: SQL → Blob
            ("SqlCustomersTable", "BlobStagingCustomers"),
            ("SqlOrdersTable", "BlobStagingOrders"),
            # Stage 2: Blob → Synapse
            ("BlobStagingCustomers", "SynapseCustomersDim"),
            ("BlobStagingOrders", "SynapseOrdersFact"),
            # Stage 3: Blob → DataLake
            ("BlobStagingCustomers", "DataLakeRawData"),
        ],
    }


# =============================================================================
# SCENARIO 6: DIVERSE ACTIVITY TYPES
# =============================================================================


def create_diverse_activities_scenario() -> Dict[str, Any]:
    """Create mock data for testing various activity types.

    Structure:
    - DiverseActivitiesPipeline
      └── SetVariable: InitializeCounter
      └── WebActivity: FetchConfiguration (REST API call)
      └── SqlServerStoredProcedure: ProcessData
      └── Wait: DelayForReplication
      └── DatabricksNotebook: RunMLTraining
      └── AzureFunctionActivity: SendNotification
      └── Fail: FailOnError (in error handling)
    """
    pipeline = {
        "id": _base_resource_id("pipelines", "DiverseActivitiesPipeline"),
        "name": "DiverseActivitiesPipeline",
        "type": "Microsoft.DataFactory/factories/pipelines",
        "properties": {
            "description": "Pipeline demonstrating various activity types",
            "variables": {
                "counter": {"type": "Integer", "defaultValue": 0},
                "configData": {"type": "String"},
            },
            "activities": [
                # SetVariable - Initialize a pipeline variable
                {
                    "name": "InitializeCounter",
                    "type": "SetVariable",
                    "typeProperties": {
                        "variableName": "counter",
                        "value": {"value": "1", "type": "Expression"},
                    },
                },
                # WebActivity - Call an external REST API
                {
                    "name": "FetchConfiguration",
                    "type": "WebActivity",
                    "dependsOn": [
                        {
                            "activity": "InitializeCounter",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "typeProperties": {
                        "url": "https://api.example.com/config",
                        "method": "GET",
                        "headers": {"Content-Type": "application/json"},
                    },
                },
                # SqlServerStoredProcedure - Execute a stored procedure
                {
                    "name": "ProcessDataWithSP",
                    "type": "SqlServerStoredProcedure",
                    "dependsOn": [
                        {
                            "activity": "FetchConfiguration",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "linkedServiceName": {
                        "referenceName": "SqlServerSource",
                        "type": "LinkedServiceReference",
                    },
                    "typeProperties": {
                        "storedProcedureName": "sp_ProcessDailyData",
                        "storedProcedureParameters": {
                            "ProcessDate": {
                                "value": "@utcnow()",
                                "type": "DateTime",
                            }
                        },
                    },
                },
                # Wait - Introduce a delay
                {
                    "name": "WaitForReplication",
                    "type": "Wait",
                    "dependsOn": [
                        {
                            "activity": "ProcessDataWithSP",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "typeProperties": {"waitTimeInSeconds": 30},
                },
                # GetMetadata - Get file/folder metadata
                {
                    "name": "CheckOutputExists",
                    "type": "GetMetadata",
                    "dependsOn": [
                        {
                            "activity": "WaitForReplication",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "typeProperties": {
                        "dataset": {
                            "referenceName": "BlobStagingCustomers",
                            "type": "DatasetReference",
                        },
                        "fieldList": ["exists", "itemName", "lastModified"],
                    },
                },
                # DatabricksNotebook - Run a Databricks notebook
                {
                    "name": "RunMLTrainingNotebook",
                    "type": "DatabricksNotebook",
                    "dependsOn": [
                        {
                            "activity": "CheckOutputExists",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "linkedServiceName": {
                        "referenceName": "DatabricksWorkspace",
                        "type": "LinkedServiceReference",
                    },
                    "typeProperties": {
                        "notebookPath": "/Shared/MLTraining/train_model",
                        "baseParameters": {
                            "input_path": "/mnt/data/input",
                            "output_path": "/mnt/data/output",
                        },
                    },
                },
                # Script - Run a SQL script
                {
                    "name": "RunAnalyticsScript",
                    "type": "Script",
                    "dependsOn": [
                        {
                            "activity": "RunMLTrainingNotebook",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "linkedServiceName": {
                        "referenceName": "SynapseDestination",
                        "type": "LinkedServiceReference",
                    },
                    "typeProperties": {
                        "scripts": [
                            {
                                "text": "EXEC sp_UpdateAnalytics @date = GETDATE()",
                                "type": "Query",
                            }
                        ]
                    },
                },
                # AzureFunctionActivity - Call an Azure Function
                {
                    "name": "SendCompletionNotification",
                    "type": "AzureFunctionActivity",
                    "dependsOn": [
                        {
                            "activity": "RunAnalyticsScript",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "linkedServiceName": {
                        "referenceName": "NotificationFunction",
                        "type": "LinkedServiceReference",
                    },
                    "typeProperties": {
                        "functionName": "SendNotification",
                        "method": "POST",
                        "body": {
                            "value": '@json(concat(\'{"status": "success", "pipeline": "\', pipeline().Pipeline, \'"}\'))',
                            "type": "Expression",
                        },
                    },
                },
                # Fail - Explicitly fail the pipeline (usually in error handling)
                # Note: In real scenarios, this would be in an error handling path
                {
                    "name": "FailOnCriticalError",
                    "type": "Fail",
                    "dependsOn": [
                        {
                            "activity": "SendCompletionNotification",
                            "dependencyConditions": ["Failed"],
                        }
                    ],
                    "typeProperties": {
                        "message": "Pipeline failed due to notification error",
                        "errorCode": "500",
                    },
                },
            ],
        },
    }

    # Add Databricks linked service for the test
    databricks_linked_service = {
        "id": _base_resource_id("linkedservices", "DatabricksWorkspace"),
        "name": "DatabricksWorkspace",
        "type": "Microsoft.DataFactory/factories/linkedservices",
        "properties": {
            "type": "AzureDatabricks",
            "typeProperties": {
                "domain": "https://adb-123456789.azuredatabricks.net",
                "workspaceResourceId": "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Databricks/workspaces/my-workspace",
            },
        },
    }

    # Add Azure Function linked service
    function_linked_service = {
        "id": _base_resource_id("linkedservices", "NotificationFunction"),
        "name": "NotificationFunction",
        "type": "Microsoft.DataFactory/factories/linkedservices",
        "properties": {
            "type": "AzureFunction",
            "typeProperties": {
                "functionAppUrl": "https://my-function-app.azurewebsites.net",
            },
        },
    }

    return {
        "pipelines": [pipeline],
        "additional_linked_services": [
            databricks_linked_service,
            function_linked_service,
        ],
        "expected_datajobs": 9,  # All activities
        "activity_types_covered": [
            "SetVariable",
            "WebActivity",
            "SqlServerStoredProcedure",
            "Wait",
            "GetMetadata",
            "DatabricksNotebook",
            "Script",
            "AzureFunctionActivity",
            "Fail",
        ],
    }


# =============================================================================
# SCENARIO 7: MIXED DEPENDENCIES (Pipeline + Dataset Lineage)
# =============================================================================


def create_mixed_dependencies_scenario() -> Dict[str, Any]:
    """Create mock data for mixed pipeline and dataset dependencies.

    This scenario tests both types of lineage in a single orchestration:
    1. Pipeline-to-pipeline lineage (ExecutePipeline activities)
    2. Dataset lineage (Copy activities with inputs/outputs)

    Structure:
    - MixedOrchestrationPipeline
      └── ExecutePipeline: ExtractDataPipeline (child)
          └── Copy: ExtractFromSource (reads SqlCustomersTable, writes BlobStagingCustomers)
      └── Copy: TransformInMain (reads BlobStagingCustomers, writes SynapseCustomersDim)
      └── ExecutePipeline: LoadDataPipeline (child)
          └── Copy: LoadToDestination (reads SynapseCustomersDim, writes DataLakeCuratedData)

    Expected lineage:
    - ExecuteExtract -> ExtractFromSource (pipeline lineage)
    - TransformInMain -> BlobStagingCustomers (dataset input)
    - TransformInMain -> SynapseCustomersDim (dataset output)
    - ExecuteLoad -> LoadToDestination (pipeline lineage)
    """
    # Child pipeline for extraction
    extract_pipeline = {
        "id": _base_resource_id("pipelines", "ExtractDataPipeline"),
        "name": "ExtractDataPipeline",
        "type": "Microsoft.DataFactory/factories/pipelines",
        "properties": {
            "description": "Child pipeline for extracting data from source",
            "activities": [
                {
                    "name": "ExtractFromSource",
                    "type": "Copy",
                    "inputs": [
                        {
                            "referenceName": "SqlCustomersTable",
                            "type": "DatasetReference",
                        }
                    ],
                    "outputs": [
                        {
                            "referenceName": "BlobStagingCustomers",
                            "type": "DatasetReference",
                        }
                    ],
                    "typeProperties": {
                        "source": {"type": "AzureSqlSource"},
                        "sink": {"type": "DelimitedTextSink"},
                    },
                }
            ],
        },
    }

    # Child pipeline for loading
    load_pipeline = {
        "id": _base_resource_id("pipelines", "LoadDataPipeline"),
        "name": "LoadDataPipeline",
        "type": "Microsoft.DataFactory/factories/pipelines",
        "properties": {
            "description": "Child pipeline for loading data to destination",
            "activities": [
                {
                    "name": "LoadToDestination",
                    "type": "Copy",
                    "inputs": [
                        {
                            "referenceName": "SynapseCustomersDim",
                            "type": "DatasetReference",
                        }
                    ],
                    "outputs": [
                        {
                            "referenceName": "DataLakeCuratedData",
                            "type": "DatasetReference",
                        }
                    ],
                    "typeProperties": {
                        "source": {"type": "SqlDWSource"},
                        "sink": {"type": "ParquetSink"},
                    },
                }
            ],
        },
    }

    # Main orchestration pipeline with both ExecutePipeline and Copy activities
    main_pipeline = {
        "id": _base_resource_id("pipelines", "MixedOrchestrationPipeline"),
        "name": "MixedOrchestrationPipeline",
        "type": "Microsoft.DataFactory/factories/pipelines",
        "properties": {
            "description": "Pipeline demonstrating both pipeline and dataset dependencies",
            "activities": [
                # Step 1: Call child pipeline to extract data
                {
                    "name": "ExecuteExtract",
                    "type": "ExecutePipeline",
                    "typeProperties": {
                        "pipeline": {
                            "referenceName": "ExtractDataPipeline",
                            "type": "PipelineReference",
                        },
                        "waitOnCompletion": True,
                    },
                },
                # Step 2: Transform data in main pipeline (has dataset lineage)
                {
                    "name": "TransformInMain",
                    "type": "Copy",
                    "dependsOn": [
                        {
                            "activity": "ExecuteExtract",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "inputs": [
                        {
                            "referenceName": "BlobStagingCustomers",
                            "type": "DatasetReference",
                        }
                    ],
                    "outputs": [
                        {
                            "referenceName": "SynapseCustomersDim",
                            "type": "DatasetReference",
                        }
                    ],
                    "typeProperties": {
                        "source": {"type": "DelimitedTextSource"},
                        "sink": {"type": "SqlDWSink"},
                    },
                },
                # Step 3: Call child pipeline to load data
                {
                    "name": "ExecuteLoad",
                    "type": "ExecutePipeline",
                    "dependsOn": [
                        {
                            "activity": "TransformInMain",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "typeProperties": {
                        "pipeline": {
                            "referenceName": "LoadDataPipeline",
                            "type": "PipelineReference",
                        },
                        "waitOnCompletion": True,
                    },
                },
            ],
        },
    }

    return {
        "pipelines": [main_pipeline, extract_pipeline, load_pipeline],
        "expected_dataflows": 3,  # 3 pipelines
        "expected_datajobs": 5,  # 2 ExecutePipeline + 1 Copy in main + 2 Copy in children
        "expected_pipeline_lineage": 2,  # 2 ExecutePipeline activities
        "expected_dataset_lineage": 3,  # TransformInMain (1 in, 1 out) + ExtractFromSource + LoadToDestination
    }


# =============================================================================
# FACTORY HELPER
# =============================================================================


def create_complex_factory() -> Dict[str, Any]:
    """Create the factory that contains all complex scenarios."""
    return {
        "id": f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/{FACTORY_NAME}",
        "name": FACTORY_NAME,
        "type": "Microsoft.DataFactory/factories",
        "location": LOCATION,
        "tags": {"environment": "test", "purpose": "complex-integration-tests"},
        "properties": {
            "provisioningState": "Succeeded",
            "createTime": "2024-01-01T00:00:00Z",
        },
    }


def get_all_complex_pipelines() -> List[Dict[str, Any]]:
    """Get all pipelines from all complex scenarios."""
    pipelines = []
    pipelines.extend(create_nested_pipeline_scenario()["pipelines"])
    pipelines.extend(create_foreach_loop_scenario()["pipelines"])
    pipelines.extend(create_branching_scenario()["pipelines"])
    pipelines.extend(create_dataflow_scenario()["pipelines"])
    pipelines.extend(create_multisource_chain_scenario()["pipelines"])
    return pipelines


def get_all_data_flows() -> List[Dict[str, Any]]:
    """Get all data flows from scenarios that have them."""
    data_flows = []
    dataflow_scenario = create_dataflow_scenario()
    if "data_flows" in dataflow_scenario:
        data_flows.extend(dataflow_scenario["data_flows"])
    return data_flows
