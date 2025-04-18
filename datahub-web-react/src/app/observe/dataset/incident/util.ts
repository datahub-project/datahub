import { UnionType } from '../../../search/utils/constants';
import { HAS_ACTIVE_INCIDENTS_FILTER_FIELD, ACTIVE_INCIDENT_TYPES_FILTER_FIELD } from './constants';

export const buildIncidentTypeFilters = (selectedIncidentTypes) => {
    if (selectedIncidentTypes) {
        return {
            unionType: UnionType.OR,
            filters: selectedIncidentTypes.map((incidentType) => ({
                field: ACTIVE_INCIDENT_TYPES_FILTER_FIELD,
                value: incidentType,
            })),
        };
    }
    return {
        unionType: UnionType.AND,
        filters: [
            {
                field: HAS_ACTIVE_INCIDENTS_FILTER_FIELD,
                value: 'true',
            },
        ],
    };
};
