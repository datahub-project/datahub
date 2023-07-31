import { IncidentType } from '../../../../types.generated';

export const ACTIVE_INCIDENT_TYPES_FILTER_FIELD = 'activeIncidentTypes';
export const HAS_ACTIVE_INCIDENTS_FILTER_FIELD = 'hasActiveIncidents';

export const INCIDENT_TYPE_OPTIONS = [
    {
        name: 'Operational',
        value: IncidentType.Operational,
    },
    {
        name: 'Freshness',
        value: IncidentType.Freshness,
    },
    {
        name: 'Other',
        value: IncidentType.Custom,
    },
];

export const NAME_TO_VALUE = new Map();
INCIDENT_TYPE_OPTIONS.forEach((option) => NAME_TO_VALUE.set(option.name, option.value));

export const TYPE_TO_DISPLAY_NAME = new Map();
INCIDENT_TYPE_OPTIONS.forEach((option) => TYPE_TO_DISPLAY_NAME.set(option.value, option.name));
