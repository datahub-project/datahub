import { IncidentPriority, IncidentStage, IncidentState, IncidentType } from '@src/types.generated';

export const INCIDENT_DEFAULT_FILTERS = {
    sortBy: '',
    groupBy: 'priority',
    filterCriteria: {
        searchText: '',
        priority: [],
        stage: [],
        type: [],
        state: [IncidentState.Active],
    },
};

export const INCIDENT_GROUP_BY_FILTER_OPTIONS = [
    { label: 'Priority', value: 'priority' },
    { label: 'Stage', value: 'stage' },
    { label: 'Category', value: 'type' },
    { label: 'State', value: 'state' },
];

export const INCIDENT_TYPE_NAME_MAP = {
    CUSTOM: 'Custom',
    FIELD: 'Column',
    FRESHNESS: 'Freshness',
    DATASET: 'Other',
    DATA_SCHEMA: 'Schema',
    OPERATIONAL: 'Operational',
    SQL: 'SQL',
    VOLUME: 'Volume',
};

export const INCIDENT_CATEGORIES = [
    {
        label: INCIDENT_TYPE_NAME_MAP.OPERATIONAL,
        value: IncidentType.Operational,
    },
    {
        label: INCIDENT_TYPE_NAME_MAP.DATA_SCHEMA,
        value: IncidentType.DataSchema,
    },
    {
        label: INCIDENT_TYPE_NAME_MAP.FIELD,
        value: IncidentType.Field,
    },
    {
        label: INCIDENT_TYPE_NAME_MAP.FRESHNESS,
        value: IncidentType.Freshness,
    },
    {
        label: INCIDENT_TYPE_NAME_MAP.SQL,
        value: IncidentType.Sql,
    },
    {
        label: INCIDENT_TYPE_NAME_MAP.VOLUME,
        value: IncidentType.Volume,
    },
    {
        label: INCIDENT_TYPE_NAME_MAP.CUSTOM,
        value: IncidentType.Custom,
    },
];

export enum IncidentAction {
    CREATE = 'create',
    EDIT = 'edit',
}

interface IncidentPriorityInterface {
    label: string;
    value: IncidentPriority;
}

export const INCIDENT_PRIORITIES: IncidentPriorityInterface[] = [
    {
        label: 'Critical',
        value: IncidentPriority.Critical,
    },
    {
        label: 'High',
        value: IncidentPriority.High,
    },
    {
        label: 'Medium',
        value: IncidentPriority.Medium,
    },
    {
        label: 'Low',
        value: IncidentPriority.Low,
    },
];

export const INCIDENT_STAGES = [
    {
        label: 'Triage',
        value: IncidentStage.Triage,
    },
    {
        label: 'Investigation',
        value: IncidentStage.Investigation,
    },
    {
        label: 'In Progress',
        value: IncidentStage.WorkInProgress,
    },
    {
        label: 'Fixed',
        value: IncidentStage.Fixed,
    },
    {
        label: 'No Action',
        value: IncidentStage.NoActionRequired,
    },
];

export const INCIDENT_RESOLUTION_STAGES = [
    {
        label: 'Fixed',
        value: IncidentStage.Fixed,
    },
    {
        label: 'No Action',
        value: IncidentStage.NoActionRequired,
    },
];

export const INCIDENT_STATES = [
    {
        label: 'Resolved',
        value: IncidentState.Resolved,
    },
    {
        label: 'Active',
        value: IncidentState.Active,
    },
];

export const INCIDENT_OPTION_LABEL_MAPPING = {
    category: {
        label: 'Category',
        name: 'type',
        fieldName: 'type',
    },
    priority: {
        label: 'Priority',
        name: 'priority',
        fieldName: 'priority',
    },
    stage: {
        label: 'Stage',
        name: 'status',
        fieldName: 'status',
    },
    state: {
        label: 'Status',
        name: 'state',
        fieldName: 'state',
    },
};

export const INCIDENT_STAGE_NAME_MAP = {
    FIXED: 'Fixed',
    INVESTIGATION: 'Investigation',
    NO_ACTION_REQUIRED: 'No Action',
    TRIAGE: 'Triage',
    WORK_IN_PROGRESS: 'In progress',
    None: 'None',
};

export const INCIDENT_STATE_NAME_MAP = {
    ACTIVE: 'Active',
    RESOLVED: 'Resolved',
};

export const INCIDENT_PRIORITY_NAME_MAP = {
    CRITICAL: 'Critical',
    HIGH: 'High',
    LOW: 'Low',
    MEDIUM: 'Medium',
    None: 'None',
};

export const INCIDENT_TYPES = [
    IncidentType.Custom,
    IncidentType.DataSchema,
    IncidentType.Field,
    IncidentType.Freshness,
    IncidentType.Operational,
    IncidentType.Sql,
    IncidentType.Volume,
];

export const INCIDENT_STAGES_TYPES = [
    IncidentStage.Fixed,
    IncidentStage.Investigation,
    IncidentStage.NoActionRequired,
    IncidentStage.Triage,
    IncidentStage.WorkInProgress,
];

export const INCIDENT_STATES_TYPES = [IncidentState.Active, IncidentState.Resolved];

export const INCIDENT_PRIORITY = [
    IncidentPriority.Critical,
    IncidentPriority.High,
    IncidentPriority.Medium,
    IncidentPriority.Low,
];

export const PRIORITY_ORDER = [INCIDENT_PRIORITY_NAME_MAP.None, ...INCIDENT_PRIORITY];

export const STAGE_ORDER = [
    INCIDENT_PRIORITY_NAME_MAP.None,
    INCIDENT_STAGE_NAME_MAP.TRIAGE,
    INCIDENT_STAGE_NAME_MAP.INVESTIGATION,
    INCIDENT_STAGE_NAME_MAP.WORK_IN_PROGRESS,
    INCIDENT_STAGE_NAME_MAP.FIXED,
    INCIDENT_STAGE_NAME_MAP.NO_ACTION_REQUIRED,
];
export const STATE_ORDER = [INCIDENT_STATE_NAME_MAP.ACTIVE, INCIDENT_STATE_NAME_MAP.RESOLVED];

export const MAX_VISIBLE_ASSIGNEE = 5;

export const noPermissionsMessage = 'You do not have permission to edit incidents for this asset.';
