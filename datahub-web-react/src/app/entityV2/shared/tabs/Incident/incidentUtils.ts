import { GetEntityIncidentsDocument } from '../../../../../graphql/incident.generated';

import { IncidentType, IncidentState, Incident } from '../../../../../types.generated';

export const PAGE_SIZE = 100;

export const INCIDENT_DISPLAY_TYPES = [
    {
        type: IncidentType.Operational,
        name: 'Operational',
    },
    {
        type: 'OTHER',
        name: 'Custom',
    },
];

export const INCIDENT_DISPLAY_STATES = [
    {
        type: undefined,
        name: 'All',
    },
    {
        type: IncidentState.Active,
        name: 'Active',
    },
    {
        type: IncidentState.Resolved,
        name: 'Resolved',
    },
];

const incidentTypeToDetails = new Map();
INCIDENT_DISPLAY_TYPES.forEach((incidentDetails) => {
    incidentTypeToDetails.set(incidentDetails.type, incidentDetails);
});

export const getNameFromType = (type: IncidentType) => {
    return incidentTypeToDetails.get(type)?.name || type;
};

export const SUCCESS_COLOR_HEX = '#52C41A';
export const FAILURE_COLOR_HEX = '#F5222D';
export const WARNING_COLOR_HEX = '#FA8C16';

// apollo caching
export const addOrUpdateIncidentInList = (existingIncidents, newIncidents) => {
    const incidents = [...existingIncidents];
    let didUpdate = false;
    const updatedIncidents = incidents.map((incident) => {
        if (incident.urn === newIncidents.urn) {
            didUpdate = true;
            return newIncidents;
        }
        return { incident, siblings: null };
    });
    return didUpdate ? updatedIncidents : [newIncidents, ...existingIncidents];
};

/**
 * Add an entry to the ListIncident cache.
 */
export const updateListIncidentsCache = (client, urn, incident, pageSize) => {
    // Read the data from our cache for this query.
    const currData: any = client.readQuery({
        query: GetEntityIncidentsDocument,
        variables: {
            urn,
            start: 0,
            count: pageSize,
        },
    });

    // Add our new incidents into the existing list.
    const existingIncidents = [...(currData?.entity?.incidents?.incidents || [])];
    const newIncidents = addOrUpdateIncidentInList(existingIncidents, incident);
    const didAddIncident = newIncidents.length > existingIncidents.length;

    // Write our data back to the cache.
    client.writeQuery({
        query: GetEntityIncidentsDocument,
        variables: {
            urn,
            start: 0,
            count: pageSize,
        },
        data: {
            entity: {
                ...currData?.entity,
                incidents: {
                    __typename: 'EntityIncidentsResult',
                    start: 0,
                    count: didAddIncident
                        ? (currData?.entity?.incidents?.count || 0) + 1
                        : currData?.entity?.incidents?.count,
                    total: didAddIncident
                        ? (currData?.entity?.incidents?.total || 0) + 1
                        : currData?.entity?.incidents?.total,
                    incidents: newIncidents,
                },
                // Add the missing 'siblings' field with the appropriate data
                siblings: currData?.entity?.siblings || null,
                siblingsSearch: currData?.entity?.siblingsSearch || null,
            },
        },
    });
};

/**
 * Returns a status summary for the incidents
 */
export const getIncidentsStatusSummary = (incidents: Array<Incident>) => {
    const summary = {
        resolvedIncident: 0,
        activeIncident: 0,
        totalIncident: 0,
    };
    incidents.forEach((assertion) => {
        if (incidents.length) {
            const resultType = assertion.status.state;
            if (IncidentState.Active === resultType) {
                summary.activeIncident++;
            }
            if (IncidentState.Resolved === resultType) {
                summary.resolvedIncident++;
            }
            summary.totalIncident++;
        }
    });
    return summary;
};

/**
 * Add raised incident to cache
 */
export const addActiveIncidentToCache = (client, urn, incident, pageSize) => {
    // Add to active and overall list
    updateListIncidentsCache(client, urn, incident, pageSize);
};
