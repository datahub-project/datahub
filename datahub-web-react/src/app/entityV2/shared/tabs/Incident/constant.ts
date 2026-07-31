import i18next from 'i18next';

import { IncidentPriority, IncidentStage, IncidentState, IncidentType } from '@src/types.generated';

export const INCIDENT_DEFAULT_FILTERS = {
    sortBy: '',
    groupBy: 'priority',
    filterCriteria: {
        searchText: '',
        priority: [],
        stage: [],
        category: [],
        state: [IncidentState.Active],
    },
};

export const INCIDENT_GROUP_BY_FILTER_OPTIONS = [
    {
        get label() {
            return i18next.t('entity.profile.incident:field.priorityLabel');
        },
        value: 'priority',
    },
    {
        get label() {
            return i18next.t('entity.profile.incident:field.stageLabel');
        },
        value: 'stage',
    },
    {
        get label() {
            return i18next.t('common.labels:category');
        },
        value: 'category',
    },
    {
        get label() {
            return i18next.t('common.labels:state');
        },
        value: 'state',
    },
];

export const INCIDENT_TYPE_NAME_MAP = {
    get CUSTOM() {
        return i18next.t('entity.profile.incident:type.custom');
    },
    get FIELD() {
        return i18next.t('entity.profile.incident:type.field');
    },
    get FRESHNESS() {
        return i18next.t('entity.profile.incident:type.freshness');
    },
    get DATASET() {
        return i18next.t('entity.profile.incident:type.dataset');
    },
    get DATA_SCHEMA() {
        return i18next.t('entity.profile.incident:type.schema');
    },
    get OPERATIONAL() {
        return i18next.t('entity.profile.incident:type.operational');
    },
    SQL: 'SQL',
    get VOLUME() {
        return i18next.t('entity.profile.incident:type.volume');
    },
};

export const INCIDENT_CATEGORIES = [
    {
        get label() {
            return INCIDENT_TYPE_NAME_MAP.OPERATIONAL;
        },
        value: IncidentType.Operational,
    },
    {
        get label() {
            return INCIDENT_TYPE_NAME_MAP.DATA_SCHEMA;
        },
        value: IncidentType.DataSchema,
    },
    {
        get label() {
            return INCIDENT_TYPE_NAME_MAP.FIELD;
        },
        value: IncidentType.Field,
    },
    {
        get label() {
            return INCIDENT_TYPE_NAME_MAP.FRESHNESS;
        },
        value: IncidentType.Freshness,
    },
    {
        get label() {
            return INCIDENT_TYPE_NAME_MAP.SQL;
        },
        value: IncidentType.Sql,
    },
    {
        get label() {
            return INCIDENT_TYPE_NAME_MAP.VOLUME;
        },
        value: IncidentType.Volume,
    },
    {
        get label() {
            return INCIDENT_TYPE_NAME_MAP.CUSTOM;
        },
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
        get label() {
            return i18next.t('entity.profile.incident:priority.critical');
        },
        value: IncidentPriority.Critical,
    },
    {
        get label() {
            return i18next.t('entity.profile.incident:priority.high');
        },
        value: IncidentPriority.High,
    },
    {
        get label() {
            return i18next.t('entity.profile.incident:priority.medium');
        },
        value: IncidentPriority.Medium,
    },
    {
        get label() {
            return i18next.t('entity.profile.incident:priority.low');
        },
        value: IncidentPriority.Low,
    },
];

export const INCIDENT_STAGES = [
    {
        get label() {
            return i18next.t('entity.profile.incident:stage.triage');
        },
        value: IncidentStage.Triage,
    },
    {
        get label() {
            return i18next.t('entity.profile.incident:stage.investigation');
        },
        value: IncidentStage.Investigation,
    },
    {
        get label() {
            return i18next.t('entity.profile.incident:stage.workInProgress');
        },
        value: IncidentStage.WorkInProgress,
    },
    {
        get label() {
            return i18next.t('entity.profile.incident:stage.fixed');
        },
        value: IncidentStage.Fixed,
    },
    {
        get label() {
            return i18next.t('entity.profile.incident:stage.noAction');
        },
        value: IncidentStage.NoActionRequired,
    },
];

export const INCIDENT_RESOLUTION_STAGES = [
    {
        get label() {
            return i18next.t('entity.profile.incident:stage.fixed');
        },
        value: IncidentStage.Fixed,
    },
    {
        get label() {
            return i18next.t('entity.profile.incident:stage.noAction');
        },
        value: IncidentStage.NoActionRequired,
    },
];

export const INCIDENT_STATES = [
    {
        get label() {
            return i18next.t('entity.profile.incident:state.resolved');
        },
        value: IncidentState.Resolved,
    },
    {
        get label() {
            return i18next.t('entity.profile.incident:state.active');
        },
        value: IncidentState.Active,
    },
];

export const INCIDENT_OPTION_LABEL_MAPPING = {
    category: {
        key: 'category',
        get label() {
            return i18next.t('common.labels:category');
        },
        name: 'type',
        fieldName: 'type',
    },
    priority: {
        key: 'priority',
        get label() {
            return i18next.t('entity.profile.incident:field.priorityLabel');
        },
        name: 'priority',
        fieldName: 'priority',
    },
    stage: {
        key: 'stage',
        get label() {
            return i18next.t('entity.profile.incident:field.stageLabel');
        },
        name: 'status',
        fieldName: 'status',
    },
    state: {
        key: 'status',
        get label() {
            return i18next.t('common.labels:status');
        },
        name: 'state',
        fieldName: 'state',
    },
};

export const INCIDENT_STAGE_NAME_MAP = {
    get FIXED() {
        return i18next.t('entity.profile.incident:stage.fixed');
    },
    get INVESTIGATION() {
        return i18next.t('entity.profile.incident:stage.investigation');
    },
    get NO_ACTION_REQUIRED() {
        return i18next.t('entity.profile.incident:stage.noAction');
    },
    get TRIAGE() {
        return i18next.t('entity.profile.incident:stage.triage');
    },
    get WORK_IN_PROGRESS() {
        return i18next.t('entity.profile.incident:stage.workInProgress');
    },
    None: 'None',
};

export const INCIDENT_STATE_NAME_MAP = {
    get ACTIVE() {
        return i18next.t('entity.profile.incident:state.active');
    },
    get RESOLVED() {
        return i18next.t('entity.profile.incident:state.resolved');
    },
};

export const INCIDENT_PRIORITY_NAME_MAP = {
    get CRITICAL() {
        return i18next.t('entity.profile.incident:priority.critical');
    },
    get HIGH() {
        return i18next.t('entity.profile.incident:priority.high');
    },
    get LOW() {
        return i18next.t('entity.profile.incident:priority.low');
    },
    get MEDIUM() {
        return i18next.t('entity.profile.incident:priority.medium');
    },
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

export const PRIORITY_ORDER = ['None', ...INCIDENT_PRIORITY];

export const STAGE_ORDER = [
    'None',
    IncidentStage.Triage,
    IncidentStage.Investigation,
    IncidentStage.WorkInProgress,
    IncidentStage.Fixed,
    IncidentStage.NoActionRequired,
];
export const STATE_ORDER = [IncidentState.Active, IncidentState.Resolved];

export const MAX_VISIBLE_ASSIGNEE = 5;
