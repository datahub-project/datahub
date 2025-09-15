import { INCIDENT_TYPE_OPTIONS } from '@app/observe/dataset/incident/constants';
import { QueryParamDecoder, QueryParamEncoder } from '@app/observe/dataset/shared/util';
import { TAGS_FILTER_NAME } from '@app/searchV2/utils/constants';

import { EntityPreviewFragment } from '@graphql/preview.generated';
import { FacetFilterInput, Incident, IncidentStage, IncidentState } from '@types';

export const DEFAULT_PAGE_SIZE = 25;
export const INCIDENTS_DOCS_LINK = 'https://docs.datahub.com/docs/incidents/incidents';

export type IncidentStatusOptions = 'Active' | 'Resolved';
export type IncidentPriorityOptions = 'Critical' | 'High' | 'Medium' | 'Low';
export const STATUS_OPTIONS: IncidentStatusOptions[] = ['Active', 'Resolved'];
export const PRIORITY_OPTIONS: IncidentPriorityOptions[] = ['Critical', 'High', 'Medium', 'Low'];

export const DEFAULT_STATUS_OPTIONS: IncidentStatusOptions[] = ['Active'];
export const STATUS_OPTIONS_TO_LABEL: Record<IncidentStatusOptions, string> = {
    Active: 'Active',
    Resolved: 'Resolved',
};

export const STATUS_OPTIONS_TO_STATE: Record<IncidentStatusOptions, IncidentState> = {
    Active: IncidentState.Active,
    Resolved: IncidentState.Resolved,
};

export const DEFAULT_STAGE_OPTIONS: IncidentStage[] = [];
export const INCIDENT_STAGE_OPTIONS: IncidentStage[] = [
    IncidentStage.Triage,
    IncidentStage.Investigation,
    IncidentStage.WorkInProgress,
    IncidentStage.Fixed,
    IncidentStage.NoActionRequired,
];
export const INCIDENT_STAGE_OPTIONS_TO_LABEL: Record<IncidentStage, string> = {
    TRIAGE: 'Triage',
    INVESTIGATION: 'Investigation',
    WORK_IN_PROGRESS: 'Work in Progress',
    FIXED: 'Fixed',
    NO_ACTION_REQUIRED: 'No Action Required',
};

export const PRIORITY_OPTIONS_TO_LABEL: Record<IncidentPriorityOptions, string> = {
    Critical: 'Critical',
    High: 'High',
    Medium: 'Medium',
    Low: 'Low',
};

export const PRIORITY_OPTIONS_TO_VALUE: Record<IncidentPriorityOptions, number> = {
    Critical: 0,
    High: 1,
    Medium: 2,
    Low: 3,
};

export type IncidentWithRelationships = Incident & {
    linkedAssets: {
        relationships: {
            entity: EntityPreviewFragment;
        }[];
    };
};

export type FilterOptions = {
    // pagination
    query: string;
    page: number;
    size: number;

    // incident filters
    statuses: IncidentStatusOptions[];
    priorities: IncidentPriorityOptions[];
    types: string[];
    tags: string[];
    stages: IncidentStage[];
};

export const DEFAULT_FILTER_OPTIONS: FilterOptions = {
    // pagination
    page: 1,
    size: DEFAULT_PAGE_SIZE,

    // search
    query: '',

    // incident filters
    statuses: DEFAULT_STATUS_OPTIONS,
    priorities: [],
    types: [],
    tags: [],
    stages: DEFAULT_STAGE_OPTIONS,
};

// Decodes url query params to filter options
export const FILTER_OPTIONS_DECODER: QueryParamDecoder<FilterOptions> = {
    page: (value: string) => parseInt(value, 10),
    size: (value: string) => parseInt(value, 10),
    query: (value: string) => decodeURIComponent(value),
    statuses: (value: string) =>
        value
            .split(',')
            .map((el) => decodeURIComponent(el))
            .filter((el) => STATUS_OPTIONS.includes(el as IncidentStatusOptions)) as IncidentStatusOptions[],
    priorities: (value: string) =>
        value
            .split(',')
            .map((el) => decodeURIComponent(el))
            .filter((el) => PRIORITY_OPTIONS.includes(el as IncidentPriorityOptions)) as IncidentPriorityOptions[],
    types: (value: string) => value.split(',').map((el) => decodeURIComponent(el)),
    tags: (value: string) => value.split(',').map((el) => decodeURIComponent(el)),
    stages: (value: string) =>
        value
            .split(',')
            .map((el) => decodeURIComponent(el))
            .filter((el) => INCIDENT_STAGE_OPTIONS.includes(el as IncidentStage)) as IncidentStage[],
};

// Encodes filter options to url query params
export const FILTER_OPTIONS_ENCODER: QueryParamEncoder<FilterOptions> = {
    page: (value: number) => value.toString(),
    size: (value: number) => value.toString(),
    query: (value: string) => encodeURIComponent(value),
    statuses: (value: IncidentStatusOptions[]) => value.map((el) => encodeURIComponent(el)).join(','),
    priorities: (value: IncidentPriorityOptions[]) => value.map((el) => encodeURIComponent(el)).join(','),
    types: (value: string[]) => value.map((el) => encodeURIComponent(el)).join(','),
    tags: (value: string[]) => value.map((el) => encodeURIComponent(el)).join(','),
    stages: (value: IncidentStage[]) => value.map((el) => encodeURIComponent(el)).join(','),
};

export function buildFilters(
    statuses: IncidentStatusOptions[],
    incidentStages: IncidentStage[],
    priorities: IncidentPriorityOptions[],
    incidentTypes: string[],
    incidentTags: string[],
): FacetFilterInput[] {
    const filters: FacetFilterInput[] = [];

    // Add status filter
    if (statuses.length > 0 && statuses.length !== STATUS_OPTIONS.length) {
        filters.push({
            field: 'state',
            values: statuses.map((status) => STATUS_OPTIONS_TO_STATE[status]),
        });
    }

    // Add stage filter
    if (incidentStages.length > 0 && incidentStages.length !== INCIDENT_STAGE_OPTIONS.length) {
        filters.push({
            field: 'stage',
            values: incidentStages,
        });
    }

    // Add priority filters
    if (priorities.length > 0 && priorities.length !== PRIORITY_OPTIONS.length) {
        filters.push({
            field: 'priority',
            values: priorities.map((priority) => PRIORITY_OPTIONS_TO_VALUE[priority].toString()),
        });
    }

    // Add incident type filters
    if (incidentTypes.length > 0 && incidentTypes.length !== INCIDENT_TYPE_OPTIONS.length) {
        filters.push({ field: 'type', values: incidentTypes });
    }

    // Add incident tags
    if (incidentTags.length > 0) {
        filters.push({ field: TAGS_FILTER_NAME, values: incidentTags });
    }

    return filters;
}
