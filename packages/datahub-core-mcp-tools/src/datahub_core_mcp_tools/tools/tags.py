"""add_tags and remove_tags: attach tags to assets and columns, or take them off."""

from ..arg_validation import OptionalNullableItemStrList, StrList
from ..label_writes import LabelKind, batch_modify_labels
from ..version_requirements import destructive, min_version

TAG = LabelKind(
    noun="tag",
    urn_prefix="urn:li:tag:",
    param="tag_urns",
    input_key="tagUrns",
    gql_document="tags.gql",
    add_operation="batchAddTags",
    remove_operation="batchRemoveTags",
    find_hint='Find tags with search(query="<name>", filter="entity_type = tag").',
)


@min_version(cloud="0.3.16", oss="1.4.0")
def add_tags(
    tag_urns: StrList,
    entity_urns: StrList,
    column_paths: OptionalNullableItemStrList = None,
) -> dict:
    """Add one or more tags to multiple DataHub entities or their columns (e.g., schema fields).

    This tool allows you to tag multiple entities or their columns with multiple tags in a single operation.
    Useful for bulk tagging operations like marking multiple datasets as PII, deprecated, or applying
    governance classifications. The tags must already exist: find them with
    search(filter="entity_type = tag").

    Args:
        tag_urns: List of tag URNs to add (e.g., ["urn:li:tag:PII", "urn:li:tag:Sensitive"])
        entity_urns: List of entity URNs to tag (e.g., dataset URNs, dashboard URNs)
        column_paths: Optional list of column_path identifiers (e.g., column names for schema fields).
                     Must be same length as entity_urns if provided.
                     Use None or empty string for entity-level tags.
                     For column-level tags, provide the column name (e.g., "email_address").
                     Name a nested column by its dotted path (e.g., "address.city"). The exact
                     fieldPath list_schema_fields returns (e.g., "[version=2.0].[type=string].email")
                     works too; a column name is resolved to it.

    Returns:
        Dictionary with:
        - success: True when the tags were added
        - entities_updated: How many entities (or columns) the call covered
        - message: What was done

    Examples:
        # Add tags to multiple datasets
        add_tags(
            tag_urns=["urn:li:tag:PII", "urn:li:tag:Sensitive"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ]
        )

        # Add tags to specific columns
        add_tags(
            tag_urns=["urn:li:tag:PII"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=["email", "phone_number"]
        )

        # Mix entity-level and column-level tags
        add_tags(
            tag_urns=["urn:li:tag:Deprecated"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=[None, "deprecated_column"]  # Tag whole table and a specific column
        )
    """
    return batch_modify_labels(
        TAG,
        tool="add_tags",
        label_urns=tag_urns,
        entity_urns=entity_urns,
        column_paths=column_paths,
        operation="add",
    )


@destructive
@min_version(cloud="0.3.16", oss="1.4.0")
def remove_tags(
    tag_urns: StrList,
    entity_urns: StrList,
    column_paths: OptionalNullableItemStrList = None,
) -> dict:
    """Remove one or more tags from multiple DataHub entities or their column_paths (e.g., schema fields).

    This tool allows you to untag multiple entities or their columns with multiple tags in a single operation.
    Useful for bulk tag removal operations like removing deprecated tags, correcting misapplied classifications,
    or cleaning up governance metadata.

    Args:
        tag_urns: List of tag URNs to remove (e.g., ["urn:li:tag:PII", "urn:li:tag:Sensitive"])
        entity_urns: List of entity URNs to untag (e.g., dataset URNs, dashboard URNs)
        column_paths: Optional list of column_path identifiers (e.g., column names for schema fields).
                     Must be same length as entity_urns if provided.
                     Use None or empty string for entity-level tag removal.
                     For column-level tag removal, provide the column name (e.g., "email_address").
                     Name a nested column by its dotted path (e.g., "address.city"). The exact
                     fieldPath list_schema_fields returns (e.g., "[version=2.0].[type=string].email")
                     works too; a column name is resolved to it.

    Returns:
        Dictionary with:
        - success: True when the tags were removed
        - entities_updated: How many entities (or columns) the call covered
        - message: What was done

    Examples:
        # Remove tags from multiple datasets
        remove_tags(
            tag_urns=["urn:li:tag:Deprecated", "urn:li:tag:Legacy"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_customers,PROD)"
            ]
        )

        # Remove tags from specific columns
        remove_tags(
            tag_urns=["urn:li:tag:PII"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=["old_email_field", "deprecated_phone"]
        )

        # Mix entity-level and column-level tag removal
        remove_tags(
            tag_urns=["urn:li:tag:Experimental"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.stable_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=[None, "test_column"]  # Remove from whole table and a specific column
        )
    """
    return batch_modify_labels(
        TAG,
        tool="remove_tags",
        label_urns=tag_urns,
        entity_urns=entity_urns,
        column_paths=column_paths,
        operation="remove",
    )
