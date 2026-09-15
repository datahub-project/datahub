import i18next from 'i18next';

import { GetEntityIncidentsDocument } from '@graphql/incident.generated';
import { Incident, IncidentState, IncidentType } from '@types';

export const PAGE_SIZE = 100;

export function getIncidentDisplayTypes() {
    return [
        {
            type: IncidentType.Operational,
            name: i18next.t('entity.profile.incident:type.operational'),
        },
        {
            type: 'OTHER',
            name: i18next.t('entity.profile.incident:type.custom'),
        },
    ];
}

export function getIncidentDisplayStates() {
    return [
        {
            type: undefined,
            name: i18next.t('entity.profile.incident:state.all'),
        },
        {
            type: IncidentState.Active,
            name: i18next.t('entity.profile.incident:state.active'),
        },
        {
            type: IncidentState.Resolved,
            name: i18next.t('entity.profile.incident:state.resolved'),
        },
    ];
}

export const getNameFromType = (type: IncidentType) => {
    const incidentTypeToDetails = new Map();
    getIncidentDisplayTypes().forEach((incidentDetails) => {
        incidentTypeToDetails.set(incidentDetails.type, incidentDetails);
    });
    return incidentTypeToDetails.get(type)?.name || type;
};

// apollo caching
const addOrUpdateIncidentInList = (existingIncidents, newIncidents) => {
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
            const resultType = assertion.incidentStatus?.state;
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
