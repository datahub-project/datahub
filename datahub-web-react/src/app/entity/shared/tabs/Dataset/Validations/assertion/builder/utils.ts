import {
    DatasetFreshnessSourceType,
    EntityType,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
    AssertionActionType,
} from '../../../../../../../../types.generated';
import { BIGQUERY_URN, REDSHIFT_URN, SNOWFLAKE_URN } from '../../../../../../../ingest/source/builder/constants';
import { AssertionMonitorBuilderState, AssertionActionsFormState, AssertionActionsBuilderState } from './types';
import { ASSERTION_TYPES } from './constants';

export const builderStateToUpdateFreshnessAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return {
        input: {
            type: builderState.assertion?.freshnessAssertion?.type as FreshnessAssertionType,
            schedule: {
                type: builderState.assertion?.freshnessAssertion?.schedule?.type as FreshnessAssertionScheduleType,
                cron: builderState.assertion?.freshnessAssertion?.schedule?.cron,
                fixedInterval: builderState.assertion?.freshnessAssertion?.schedule?.fixedInterval,
            },
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

export const SOURCE_TYPES = [
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
        name: 'Date Column',
        description:
            'Check the maximum value of a timestamp or date column to determine whether the dataset has changed',
    },
];

export const SOURCE_TYPE_TO_INFO = new Map();
SOURCE_TYPES.forEach((type) => {
    SOURCE_TYPE_TO_INFO.set(type.type, type);
});

export const getSourceTypesForPlatform = (platformUrn: string) => {
    switch (platformUrn) {
        case SNOWFLAKE_URN:
            return [
                DatasetFreshnessSourceType.AuditLog,
                DatasetFreshnessSourceType.InformationSchema,
                DatasetFreshnessSourceType.FieldValue,
            ];
        case BIGQUERY_URN:
            return [
                DatasetFreshnessSourceType.AuditLog,
                DatasetFreshnessSourceType.InformationSchema,
                DatasetFreshnessSourceType.FieldValue,
            ];
        case REDSHIFT_URN:
            return [DatasetFreshnessSourceType.AuditLog, DatasetFreshnessSourceType.FieldValue];
        default:
            return []; // No types supported.
    }
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
