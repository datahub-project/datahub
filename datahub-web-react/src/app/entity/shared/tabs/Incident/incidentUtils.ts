import { IncidentType, IncidentState } from '../../../../../types.generated';

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
export const FAILURE_COLOR_HEX = '#FA8C16';
