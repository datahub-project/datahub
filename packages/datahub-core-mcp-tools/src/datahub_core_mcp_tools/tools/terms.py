"""add_terms and remove_terms: attach glossary terms to assets and columns, or
take them off."""

from ..arg_validation import OptionalNullableItemStrList, StrList
from ..label_writes import LabelKind, batch_modify_labels
from ..version_requirements import destructive, idempotent, min_version

GLOSSARY_TERM = LabelKind(
    noun="glossary term",
    urn_prefix="urn:li:glossaryTerm:",
    param="term_urns",
    input_key="termUrns",
    gql_document="glossary_terms.gql",
    add_operation="batchAddTerms",
    remove_operation="batchRemoveTerms",
    find_hint=(
        'Find glossary terms with search(query="<name>", '
        'filter="entity_type = glossaryTerm").'
    ),
)


@idempotent
@min_version(cloud="0.3.16", oss="1.4.0")
def add_glossary_terms(
    term_urns: StrList,
    entity_urns: StrList,
    column_paths: OptionalNullableItemStrList = None,
) -> dict:
    """Add one or more glossary terms (terms) to multiple DataHub entities or their columns (e.g., schema fields).

    This tool allows you to associate multiple entities or their columns with multiple glossary terms in a single operation.
    Useful for bulk term assignment operations like applying business definitions, standardizing terminology,
    or enriching metadata with domain knowledge. The terms must already exist: find
    them with search(filter="entity_type = glossaryTerm").

    Args:
        term_urns: List of glossary term URNs to add (e.g., ["urn:li:glossaryTerm:CustomerData", "urn:li:glossaryTerm:SensitiveInfo"])
        entity_urns: List of entity URNs to annotate (e.g., dataset URNs, dashboard URNs)
        column_paths: Optional list of column_path identifiers (e.g., column names for schema fields).
                     Must be same length as entity_urns if provided.
                     Use None or empty string for entity-level glossary terms.
                     For column-level glossary terms, provide the column name (e.g., "customer_email").
                     Name a nested column by its dotted path (e.g., "address.city"). The exact
                     fieldPath list_schema_fields returns (e.g., "[version=2.0].[type=string].email")
                     works too; a column name is resolved to it.

    Returns:
        Dictionary with:
        - success: True when the terms were added
        - entities_updated: How many entities (or columns) the call covered
        - message: What was done

    Examples:
        # Add glossary terms to multiple datasets
        add_terms(
            term_urns=["urn:li:glossaryTerm:CustomerData", "urn:li:glossaryTerm:PersonalInformation"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ]
        )

        # Add glossary terms to specific columns
        add_terms(
            term_urns=["urn:li:glossaryTerm:EmailAddress"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=["email", "contact_email"]
        )

        # Mix entity-level and column-level glossary terms
        add_terms(
            term_urns=["urn:li:glossaryTerm:Revenue"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.transactions,PROD)"
            ],
            column_paths=[None, "total_amount"]  # Term for whole table and a specific column
        )
    """
    return batch_modify_labels(
        GLOSSARY_TERM,
        tool="add_terms",
        label_urns=term_urns,
        entity_urns=entity_urns,
        column_paths=column_paths,
        operation="add",
    )


@destructive
@idempotent
@min_version(cloud="0.3.16", oss="1.4.0")
def remove_glossary_terms(
    term_urns: StrList,
    entity_urns: StrList,
    column_paths: OptionalNullableItemStrList = None,
) -> dict:
    """Remove one or more glossary terms (terms) from multiple DataHub entities or their column_paths (e.g., schema fields).

    This tool allows you to disassociate multiple entities or their columns from multiple glossary terms in a single operation.
    Useful for bulk term removal operations like correcting misapplied business definitions, updating terminology,
    or cleaning up metadata.

    Args:
        term_urns: List of glossary term URNs to remove (e.g., ["urn:li:glossaryTerm:Deprecated", "urn:li:glossaryTerm:Legacy"])
        entity_urns: List of entity URNs to remove terms from (e.g., dataset URNs, dashboard URNs)
        column_paths: Optional list of column_path identifiers (e.g., column names for schema fields).
                     Must be same length as entity_urns if provided.
                     Use None or empty string for entity-level glossary term removal.
                     For column-level glossary term removal, provide the column name (e.g., "old_field").
                     Name a nested column by its dotted path (e.g., "address.city"). The exact
                     fieldPath list_schema_fields returns (e.g., "[version=2.0].[type=string].email")
                     works too; a column name is resolved to it.

    Returns:
        Dictionary with:
        - success: True when the terms were removed
        - entities_updated: How many entities (or columns) the call covered
        - message: What was done

    Examples:
        # Remove glossary terms from multiple datasets
        remove_terms(
            term_urns=["urn:li:glossaryTerm:Deprecated", "urn:li:glossaryTerm:LegacySystem"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_customers,PROD)"
            ]
        )

        # Remove glossary terms from specific columns
        remove_terms(
            term_urns=["urn:li:glossaryTerm:Confidential"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=["old_ssn_field", "legacy_tax_id"]
        )

        # Mix entity-level and column-level glossary term removal
        remove_terms(
            term_urns=["urn:li:glossaryTerm:Experimental"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.production_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=[None, "beta_feature"]  # Remove from whole table and a specific column
        )
    """
    return batch_modify_labels(
        GLOSSARY_TERM,
        tool="remove_terms",
        label_urns=term_urns,
        entity_urns=entity_urns,
        column_paths=column_paths,
        operation="remove",
    )
