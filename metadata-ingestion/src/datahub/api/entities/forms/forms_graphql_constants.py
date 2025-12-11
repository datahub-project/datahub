# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
