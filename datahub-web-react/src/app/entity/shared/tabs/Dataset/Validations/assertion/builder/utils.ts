import { Maybe } from 'graphql/jsutils/Maybe';
import { keyBy } from 'lodash';
import {
    DatasetFilterType,
    DatasetFreshnessSourceType,
    EntityType,
    FreshnessFieldKind,
    SchemaFieldDataType,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
    AssertionActionType,
} from '../../../../../../../../types.generated';
import { BIGQUERY_URN, REDSHIFT_URN, SNOWFLAKE_URN } from '../../../../../../../ingest/source/builder/constants';
import { AssertionMonitorBuilderState, AssertionActionsFormState, AssertionActionsBuilderState } from './types';
import { ASSERTION_TYPES, HIGH_WATERMARK_FIELD_TYPES, LAST_MODIFIED_FIELD_TYPES } from './constants';

/** Configuration object used to display each source option */
export type SourceOption = {
    type: DatasetFreshnessSourceType;
    name: string;
    description: string;
    secondaryDescription?: string;
    field?: {
        kind?: FreshnessFieldKind;
        dataTypes?: Set<SchemaFieldDataType>;
    };
};

/** Different platforms may allow only certain source types. In the future, we may want a better place to declare these. */
const allowedPlatformSourceTypes = {
    [SNOWFLAKE_URN]: [
        DatasetFreshnessSourceType.AuditLog,
        DatasetFreshnessSourceType.InformationSchema,
        DatasetFreshnessSourceType.FieldValue,
    ],
    [BIGQUERY_URN]: [
        DatasetFreshnessSourceType.AuditLog,
        DatasetFreshnessSourceType.InformationSchema,
        DatasetFreshnessSourceType.FieldValue,
    ],
    [REDSHIFT_URN]: [DatasetFreshnessSourceType.AuditLog, DatasetFreshnessSourceType.FieldValue],
};

/** Configuration object for all possible source options */
const allSourceOptions: SourceOption[] = [
    {
        type: DatasetFreshnessSourceType.AuditLog,
        name: 'Audit Log',
        description: 'Use operations logged in the platform audit log to determine whether the dataset has changed',
    },
    {
        type: DatasetFreshnessSourceType.InformationSchema,
        name: 'Information Schema',
        description:
            'Use the information schema or system metadata tables to determine whether the dataset has changed',
    },
    {
        type: DatasetFreshnessSourceType.FieldValue,
        name: 'Last Modified Column',
        description:
            'Use an audit column which represents the "last modified" time of an individual row to determine whether the dataset has changed.',
        secondaryDescription:
            'Select the column representing the latest update time for a given row. This column must have type TIMESTAMP, DATE, or DATETIME.',
        field: {
            kind: FreshnessFieldKind.LastModified,
            dataTypes: LAST_MODIFIED_FIELD_TYPES,
        },
    },
    {
        type: DatasetFreshnessSourceType.FieldValue,
        name: 'High Watermark Column',
        description:
            'Use a sortable column with a continuously increasing value, such as a partition date, timestamp, or an incrementing id, to determine whether the dataset has changed.',
        secondaryDescription:
            'Select the sortable, incrementing column used to track changes in the dataset. This column must have type INTEGER, TIMESTAMP, DATE, or DATETIME.',
        field: {
            kind: FreshnessFieldKind.HighWatermark,
            dataTypes: HIGH_WATERMARK_FIELD_TYPES,
        },
    },
];

/** Create a unique identifier for each source config option */
const getSourceOptionKey = (type: DatasetFreshnessSourceType, kind?: Maybe<FreshnessFieldKind>) => {
    return `${type}.${kind || ''}`;
};

/** Map of all source options to allow constant lookup by Source Type and Field Kind */
const sourceOptionsByKey = keyBy(allSourceOptions, ({ type, field }) => getSourceOptionKey(type, field?.kind));

export const builderStateToUpdateFreshnessAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return {
        input: {
            type: builderState.assertion?.freshnessAssertion?.type as FreshnessAssertionType,
            schedule: {
                type: builderState.assertion?.freshnessAssertion?.schedule?.type as FreshnessAssertionScheduleType,
                cron: builderState.assertion?.freshnessAssertion?.schedule?.cron,
                fixedInterval: builderState.assertion?.freshnessAssertion?.schedule?.fixedInterval,
            },
            filter: builderState.assertion?.freshnessAssertion?.filter
                ? {
                      type: builderState.assertion?.freshnessAssertion?.filter.type as DatasetFilterType,
                      sql: builderState.assertion?.freshnessAssertion?.filter.sql,
                  }
                : undefined,
            actions: builderState.assertion?.actions
                ? {
                      onSuccess: builderState.assertion?.actions?.onSuccess || [],
                      onFailure: builderState.assertion?.actions?.onFailure || [],
                  }
                : undefined,
        },
    };
};

export const builderStateToCreateAssertionMonitorVariables = (
    assertionUrn: string,
    builderState: AssertionMonitorBuilderState,
) => {
    return {
        input: {
            entityUrn: builderState?.entityUrn,
            assertionUrn,
            schedule: builderState.schedule,
            parameters: builderState.parameters,
        },
    };
};

export const builderStateToCreateFreshnessAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return {
        input: {
            entityUrn: builderState.entityUrn as string,
            ...builderStateToUpdateFreshnessAssertionVariables(builderState).input,
        },
    };
};

export const getAssertionTypesForEntityType = (entityType: EntityType) => {
    return ASSERTION_TYPES.filter((type) => type.entityTypes.includes(entityType));
};

export const getSourceOptions = (platformUrn: string) => {
    const allowedSourceTypes = allowedPlatformSourceTypes[platformUrn] || [];
    return allSourceOptions.filter((option) => allowedSourceTypes.includes(option.type));
};

export const getSourceOption = (type: DatasetFreshnessSourceType, kind?: Maybe<FreshnessFieldKind>) => {
    return sourceOptionsByKey[getSourceOptionKey(type, kind)];
};

/**
 * Returns true if the entity is eligible for online assertion monitoring.
 * Currently limited to Snowflake, Redshift, and BigQuery.
 */
const ASSERTION_SUPPORTED_PLATFORM_URNS = [SNOWFLAKE_URN, REDSHIFT_URN, BIGQUERY_URN];
export const isEntityEligibleForAssertionMonitoring = (platformUrn) => {
    if (!platformUrn) {
        return false;
    }
    return ASSERTION_SUPPORTED_PLATFORM_URNS.includes(platformUrn);
};

export const adjustCronText = (text: string) => {
    return text.replace('at', '');
};

export const toggleRaiseIncidentState = (state: AssertionActionsFormState, newValue: boolean) => {
    let newFailureActions = state.onFailure || [];
    if (newValue) {
        // Add auto-raise incident action.
        newFailureActions = [...newFailureActions, { type: AssertionActionType.RaiseIncident }];
    } else {
        // Remove auto-raise incident actions.
        newFailureActions = [
            ...newFailureActions.filter((action) => action.type !== AssertionActionType.RaiseIncident),
        ];
    }
    return {
        ...state,
        onFailure: newFailureActions,
    };
};

export const toggleResolveIncidentState = (state: AssertionActionsFormState, newValue: boolean) => {
    let newSuccessActions = state.onSuccess || [];
    if (newValue) {
        // Add auto-resolve incident action.
        newSuccessActions = [...newSuccessActions, { type: AssertionActionType.ResolveIncident }];
    } else {
        // Remove auto-raise incident actions.
        newSuccessActions = [
            ...newSuccessActions.filter((action) => action.type !== AssertionActionType.ResolveIncident),
        ];
    }
    return {
        ...state,
        onSuccess: newSuccessActions,
    };
};

export const builderStateToUpdateAssertionActionsVariables = (
    urn: string,
    builderState: AssertionActionsBuilderState,
) => {
    return {
        urn,
        input: {
            onSuccess: builderState.actions?.onSuccess || [],
            onFailure: builderState.actions?.onFailure || [],
        },
    };
};
