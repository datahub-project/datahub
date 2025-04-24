import {
    ACTIVE_INCIDENT_TYPES_FILTER_FIELD,
    HAS_ACTIVE_INCIDENTS_FILTER_FIELD,
} from '@app/observe/dataset/incident/constants';
import { UnionType } from '@app/search/utils/constants';

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
