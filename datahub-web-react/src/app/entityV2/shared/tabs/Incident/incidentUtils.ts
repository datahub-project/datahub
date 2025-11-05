import { getExistingIncidents } from '@app/entityV2/shared/tabs/Incident/utils';

import { GetEntityIncidentsDocument } from '@graphql/incident.generated';
import { IncidentType } from '@types';

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

const incidentTypeToDetails = new Map();
INCIDENT_DISPLAY_TYPES.forEach((incidentDetails) => {
    incidentTypeToDetails.set(incidentDetails.type, incidentDetails);
});

// apollo caching
const addOrUpdateIncidentInList = (existingIncidents, newIncidents) => {
    const incidents = [...existingIncidents];
    let didUpdate = false;
    const updatedIncidents = incidents.map((incident) => {
        if (incident.urn === newIncidents.urn) {
            didUpdate = true;
            return {
                ...incident,
                ...newIncidents,
            };
        }
        return incident;
    });
    return didUpdate ? updatedIncidents : [newIncidents, ...existingIncidents];
};

/**
 * Add an entry to the ListIncident cache.
 */
const updateListIncidentsCache = (client, urn, incident, pageSize) => {
    // Read the data from our cache for this query.
    const currData: any = client.readQuery({
        query: GetEntityIncidentsDocument,
        variables: {
            urn,
            start: 0,
            count: pageSize,
        },
    });

    if (!currData) {
        return;
    }

    const existingIncidents = getExistingIncidents(currData);

    // Add our new incidents into the existing list.
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
 * Add raised incident to cache
 */
export const updateActiveIncidentInCache = (client, urn, incident, pageSize) => {
    // Add to active and overall list
    updateListIncidentsCache(client, urn, incident, pageSize);
};
