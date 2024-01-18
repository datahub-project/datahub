import { GetIncidentsDocument, GetIncidentsQuery } from '../../../../../graphql/dataset.generated';
import { IncidentType, IncidentState } from '../../../../../types.generated';

export const PAGE_SIZE = 10;

export const INCIDENT_DISPLAY_TYPES = [
    {
        type: IncidentType.Operational,
        name: 'Operational',
    },
    {
        type: 'OTHER',
        name: 'Other',
    },
];

export const INCIDENT_DISPLAY_STATES = [
    {
        type: 'All',
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
        return {incident, siblings: null};
    });
    return didUpdate ? updatedIncidents : [newIncidents, ...existingIncidents];
};

/**
 * Add an entry to the ListIncident cache.
 */
export const updateListIncidentsCache = (client, incidents, pageSize) => {
    // Read the data from our cache for this query.
    const currData: GetIncidentsQuery | null = client.readQuery({
        query: GetIncidentsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
                query: undefined,
            },
        },
    });

    // Add our new incidents into the existing list.
    const existingIncidents = [...(currData?.dataset?.incidents?.incidents || [])];
    const newIncidents = addOrUpdateIncidentInList(existingIncidents, incidents);
    const didAddTest = newIncidents.length > existingIncidents.length;

    // Write our data back to the cache.
    client.writeQuery({
        query: GetIncidentsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
                query: undefined,
            },
        },
        data: {
            dataset: {
                incidents: {
                    start: 0,
                    count: didAddTest ? (currData?.dataset?.incidents?.count || 0) + 1 : currData?.dataset?.incidents?.count,
                    total: didAddTest ? (currData?.dataset?.incidents?.total || 0) + 1 : currData?.dataset?.incidents?.total,
                    incidents: newIncidents,
                },
                // Add the missing 'siblings' field with the appropriate data
                siblings: currData?.dataset?.siblings || null,
            },
        },
    });
};


