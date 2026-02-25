"""
Ordered manifest of library examples for integration testing.

Examples are grouped by entity type and ordered such that:
1. CREATE operations come before READ/UPDATE operations
2. Dependencies are respected (e.g., create glossary term before adding to dataset)
3. Each example can run independently assuming its prerequisites exist

This manifest is used by test_library_examples.py to execute examples in the correct order.
"""

# List of example scripts to execute, in order
# Format: relative path from metadata-ingestion/examples/library/
EXAMPLE_MANIFEST = [
    # Application examples
    "application_create.py",
    "application_query_rest_api.py",
    # Assertion examples
    "assertion_create_freshness.py",
    "assertion_create_volume_rows.py",
    "assertion_create_field_uniqueness.py",
    "assertion_create_schema.py",
    "assertion_create_sql_metric.py",
    # Business Attribute examples
    "business_attribute_create.py",
    "business_attribute_query.py",
    # Chart examples
    "chart_create_simple.py",
    # Container examples
    "container_create.py",
    "container_create_database.py",
    "container_create_schema.py",
    # Corp Group examples
    "corpgroup_create.py",
    "corpgroup_query_rest_api.py",
    # Corp User examples
    "corpuser_create_basic.py",
    # Dashboard examples
    "dashboard_create_simple.py",
    # Data Contract examples
    "datacontract_create_basic.py",
    # Data Platform examples
    "data_platform_create.py",
    # Data Process Instance examples
    "data_process_instance_create_simple.py",
    # Dataflow examples
    "dataflow_create.py",
    # Datajob examples
    "datajob_create_basic.py",
    # Data Product examples
    "dataproduct_create.py",
    # Domain examples
    "domain_create.py",
    "domain_create_nested.py",
    "search_filter_by_domain.py",
    # ER Model Relationship examples
    "ermodelrelationship_create_basic.py",
    # Glossary examples
    "glossary_node_create.py",
    "glossary_term_create.py",
    "glossary_term_create_simple.py",
    # Incident examples
    "incident_create.py",
    "incident_query_rest_api.py",
    # ML Feature examples
    "mlfeature_create.py",
    # ML Feature Table examples
    "mlfeature_table_create.py",
    # ML Model examples
    "mlmodel_create.py",
    "mlmodel_deployment_create.py",
    # ML Model Group examples
    "mlmodel_group_create.py",
    # ML Primary Key examples
    "mlprimarykey_create.py",
    # Notebook examples
    "notebook_create.py",
    # Ownership Type examples
    "ownership_type_create_custom.py",
    # Platform Instance examples
    "platform_instance_create.py",
    # Query examples
    "query_create.py",
    # Role examples
    "role_create.py",
    "role_query.py",
    # Structured Property examples
    "structured_property_create_basic.py",
    # Dataset examples (depends on structured property)
    "dataset_create_with_structured_properties.py",
    # Form examples - Excluded: FormPromptValidator performs search operation that may timeout
    # if search backend not properly configured. Forms feature not widely used/tested.
    # "form_create.py",
    # Subscription examples - requires acryl-datahub-cloud (cloud-only feature)
    # "subscription_create.py",
    # Tag examples
    "tag_create_basic.py",
]

# Dependencies between tests - tests that must run sequentially
# Format: {dependent_test: [prerequisite_tests]}
EXAMPLE_DEPENDENCIES = {
    "dataset_create_with_structured_properties.py": [
        "structured_property_create_basic.py"
    ],
}
