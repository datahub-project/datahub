import {
    AssertionEvaluationParametersType,
    AssertionType,
    DatasetFreshnessSourceType,
    DateInterval,
    EntityType,
    SchemaFieldDataType,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
} from '../../../../../../../../types.generated';

// Every 6 hours.
export const DEFAULT_ASSERTION_EVALUATION_SCHEDULE = '0 */6 * * *';

// Table cannot be more than 6 hours late.
export const DEFAULT_ASSERTION_EVALUATION_INTERVAL_UNIT = DateInterval.Hour;
export const DEFAULT_ASSERTION_EVALUATION_INTERVAL_MULTIPLE = 6;

// Information used for rendering different types of assertions.
export const ASSERTION_TYPES = [
    {
        name: 'Freshness',
        description: 'Monitor the freshness of this dataset by defining a custom assertion',
        imageSrc: null,
        type: AssertionType.Freshness,
        entityTypes: [EntityType.Dataset],
    },
];

export const TIMESTAMP_FIELD_TYPES = new Set([SchemaFieldDataType.Date, SchemaFieldDataType.Time]);

// Default state used to initialize the Assertion Monitor Builder.
export const DEFAULT_BUILDER_STATE = {
    entityUrn: null,
    assertion: {
        type: null,
        freshnessAssertion: null,
        actions: null,
    },
    schedule: {
        cron: DEFAULT_ASSERTION_EVALUATION_SCHEDULE, // Every 6 hours.
        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    },
    parameters: null,
};

// Default assertion definition used when the selected type is Freshness.
export const DEFAULT_DATASET_FRESHNESS_ASSERTION_STATE = {
    type: FreshnessAssertionType.DatasetChange,
    schedule: {
        type: FreshnessAssertionScheduleType.FixedInterval,
        fixedInterval: {
            unit: DateInterval.Hour,
            multiple: 6,
        },
    },
};

// Default assertion parameters used when the selected type is Freshness
export const DEFAULT_DATASET_FRESHNESS_ASSERTION_PARAMETERS_STATE = {
    type: AssertionEvaluationParametersType.DatasetFreshness,
    datasetFreshnessParameters: {
        sourceType: DatasetFreshnessSourceType.AuditLog,
        auditLog: {},
    },
};

// Default state used to initialize the Assertion Actions Builder.
export const DEFAULT_ACTIONS_BUILDER_STATE = {
    actions: null,
};
