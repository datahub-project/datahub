UPDATE_INCIDENT_STATUS_MUTATION = """
    mutation updateIncidentStatus($urn: String!, $input: IncidentStatusInput!) {
        updateIncidentStatus(urn: $urn, input: $input)
    }
"""

UPDATE_INCIDENT_PRIORITY_MUTATION = """
    mutation updateIncidentPriority($urn: String!, $priority: IncidentPriority!) {
        updateIncident(urn: $urn, input: { priority: $priority })
    }
"""
