UPLOAD_ENTITIES_FOR_FORMS = """
mutation batchAssignForm {{
    batchAssignForm(
    input: {{
        formUrn: "{form_urn}",
        entityUrns: [{entity_urns}]
    }}
    )
}}
"""

FIELD_FILTER_TEMPLATE = (
    """{{ field: "{field}", values: [{values}], condition: EQUAL, negated: false }}"""
)

CREATE_DYNAMIC_FORM_ASSIGNMENT = """
mutation createDynamicFormAssignment {{
    createDynamicFormAssignment(
    input: {{
        formUrn: "{form_urn}"
        orFilters: [{{
            and: [{filters}]
        }}]
    }}
    )
}}
"""
