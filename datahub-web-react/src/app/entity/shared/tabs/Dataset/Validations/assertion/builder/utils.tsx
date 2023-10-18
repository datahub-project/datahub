import React from 'react';
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
    AssertionEvaluationParametersType,
    VolumeAssertionType,
    AssertionStdOperator,
    AssertionStdParameters,
    AssertionValueChangeType,
    IncrementingSegmentSpecInput,
    SqlAssertionType,
    FieldAssertionType,
    SchemaField,
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
    allowedScheduleTypes: FreshnessAssertionScheduleType[];
};

/** Different platforms may allow only certain source types. In the future, we may want a better place to declare these. */
const PLATFORM_ASSERTION_CONFIGS = {
    [SNOWFLAKE_URN]: {
        freshness: {
            defaultSourceType: DatasetFreshnessSourceType.AuditLog,
            sourceTypes: [
                DatasetFreshnessSourceType.AuditLog,
                DatasetFreshnessSourceType.InformationSchema,
                DatasetFreshnessSourceType.FieldValue,
                DatasetFreshnessSourceType.DatahubOperation,
            ],
            sourceTypeDetails: {
                [DatasetFreshnessSourceType.AuditLog]: {
                    description: (
                        <>
                            We&apos;ll use Snowflake{' '}
                            <b>
                                <a
                                    href="https://docs.snowflake.com/en/sql-reference/account-usage/access_history"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                >
                                    Access History
                                </a>
                            </b>{' '}
                            view to determine whether a Table has changed. Note that this requires the Enterprise
                            Edition (or higher) of Snowflake and is only supported for Tables, not Views.
                        </>
                    ),
                },
                [DatasetFreshnessSourceType.InformationSchema]: {
                    description: (
                        <>
                            We&apos;ll use Snowflake{' '}
                            <b>
                                <a
                                    href="https://docs.snowflake.com/en/sql-reference/info-schema/tables"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                >
                                    Information Schema &gt; Tables
                                </a>
                            </b>{' '}
                            view to determine whether the Table has changed. Note that this is only supported for
                            Tables, not Views.
                        </>
                    ),
                },
                [DatasetFreshnessSourceType.FieldValue]: {
                    description: (
                        <>
                            We&apos;ll query a specific column of the Snowflake Table or View to determine whether it
                            has changed.
                            <br /> This requires that the configured user account has read access to the asset.
                        </>
                    ),
                },
            },
        },
    },
    [BIGQUERY_URN]: {
        freshness: {
            defaultSourceType: DatasetFreshnessSourceType.AuditLog,
            sourceTypes: [
                DatasetFreshnessSourceType.AuditLog,
                DatasetFreshnessSourceType.InformationSchema,
                DatasetFreshnessSourceType.FieldValue,
                DatasetFreshnessSourceType.DatahubOperation,
            ],
            sourceTypeDetails: {
                [DatasetFreshnessSourceType.AuditLog]: {
                    description: (
                        <>
                            We&apos;ll use BigQuery{' '}
                            <b>
                                <a
                                    href="https://cloud.google.com/bigquery/docs/reference/auditlogs"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                >
                                    Cloud Audit Logs
                                </a>
                            </b>{' '}
                            API to determine whether a Table has changed. This requires that your configured Service
                            Account has access to read the audit logs (permissions logging.logEntries.list and
                            logging.privateLogEntries.list). <br /> This method is only supported for Tables, not Views.
                        </>
                    ),
                },
                [DatasetFreshnessSourceType.InformationSchema]: {
                    description: (
                        <>
                            We&apos;ll use BigQuery <b>System Tables (__TABLES__)</b> to determine whether the Table has
                            changed. This requires that your configured Service Account has access to read data and
                            metadata for the Dataset (roles/bigquery.metadataViewer and roles/bigquery.dataViewer).{' '}
                            <br /> This method is only supported for Tables, not Views.
                        </>
                    ),
                },
                [DatasetFreshnessSourceType.FieldValue]: {
                    description: (
                        <>
                            We&apos;ll query a specific column of the BigQuery Table or View to determine whether it has
                            changed. <br />
                            This requires that the configured Service Account has read access to the asset.
                        </>
                    ),
                },
            },
        },
    },
    [REDSHIFT_URN]: {
        freshness: {
            defaultSourceType: DatasetFreshnessSourceType.AuditLog,
            sourceTypes: [
                DatasetFreshnessSourceType.AuditLog,
                DatasetFreshnessSourceType.FieldValue,
                DatasetFreshnessSourceType.DatahubOperation,
            ],
            sourceTypeDetails: {
                [DatasetFreshnessSourceType.AuditLog]: {
                    description: (
                        <>
                            We&apos;ll use Redshift{' '}
                            <b>
                                <a
                                    href="https://docs.aws.amazon.com/redshift/latest/dg/c_intro_STL_tables.html"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                >
                                    STL Views
                                </a>
                            </b>
                            , including <b>STL_INSERT</b>, <b>SVV_TABLE_INFO</b>, <b>STL_QUERY</b>, and{' '}
                            <b>STL_USER_INFO</b>, to determine whether a Table has changed. <br />
                            Notice that this is limited to detecting <b>INSERT</b> operations to the table; all other
                            options will be ignored. <br />
                            This mechanism is only supported for Tables, not Views.
                        </>
                    ),
                },
                [DatasetFreshnessSourceType.FieldValue]: {
                    description: (
                        <>
                            We&apos;ll query a specific column of the Redshift Table or View to determine whether it has
                            changed. <br />
                            This requires that the configured user account has read access to the asset.
                        </>
                    ),
                },
            },
        },
    },
};

/** Configuration object for all possible source options */
const allSourceOptions: SourceOption[] = [
    {
        type: DatasetFreshnessSourceType.AuditLog,
        name: 'Audit Log',
        description: 'Use operations logged in platform audit logs to determine whether the asset has changed',
        allowedScheduleTypes: [FreshnessAssertionScheduleType.FixedInterval, FreshnessAssertionScheduleType.Cron],
    },
    {
        type: DatasetFreshnessSourceType.InformationSchema,
        name: 'Information Schema',
        description: 'Use the information schema or system metadata tables to determine whether the asset has changed',
        allowedScheduleTypes: [FreshnessAssertionScheduleType.FixedInterval, FreshnessAssertionScheduleType.Cron],
    },
    {
        type: DatasetFreshnessSourceType.FieldValue,
        name: 'Last Modified Column',
        description:
            'Use an audit column which represents the "last modified" time of an individual row to determine whether the dataset has changed.',
        secondaryDescription:
            'Select a column containing the last modified time for a given row. This column must have type TIMESTAMP, DATE, or DATETIME.',
        field: {
            kind: FreshnessFieldKind.LastModified,
            dataTypes: LAST_MODIFIED_FIELD_TYPES,
        },
        allowedScheduleTypes: [FreshnessAssertionScheduleType.FixedInterval, FreshnessAssertionScheduleType.Cron],
    },
    {
        type: DatasetFreshnessSourceType.FieldValue,
        name: 'High Watermark Column',
        description:
            'Use a sortable column with a continuously increasing value, such as a partition date, timestamp, or an incrementing id, to determine whether the dataset has changed. Only available when "Since the previous check" is selected.',
        secondaryDescription:
            'Select a sortable, incrementing column used to track changes in the dataset. This column must have type INTEGER, TIMESTAMP, DATE, or DATETIME.',
        field: {
            kind: FreshnessFieldKind.HighWatermark,
            dataTypes: HIGH_WATERMARK_FIELD_TYPES,
        },
        allowedScheduleTypes: [FreshnessAssertionScheduleType.Cron],
    },
    {
        type: DatasetFreshnessSourceType.DatahubOperation,
        name: 'DataHub Operation',
        description:
            'Use the DataHub "Operation" Aspect to determine whether the table has changed. This avoids the requirement to contact your data platform to determine evaluate Freshness Assertions. Note that this relies on operations being reported to DataHub, either via ingestion or via use of the DataHub APIs (reportOperation).',
        allowedScheduleTypes: [FreshnessAssertionScheduleType.FixedInterval, FreshnessAssertionScheduleType.Cron],
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
                cron:
                    builderState.assertion?.freshnessAssertion?.schedule?.type === FreshnessAssertionScheduleType.Cron
                        ? builderState.assertion?.freshnessAssertion?.schedule?.cron
                        : undefined,
                fixedInterval:
                    builderState.assertion?.freshnessAssertion?.schedule?.type ===
                    FreshnessAssertionScheduleType.FixedInterval
                        ? builderState.assertion?.freshnessAssertion?.schedule?.fixedInterval
                        : undefined,
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

export const builderStateToVolumeTypeAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    const volumeAssertionType = builderState.assertion?.volumeAssertion?.type as VolumeAssertionType;

    switch (volumeAssertionType) {
        case VolumeAssertionType.RowCountTotal:
            return {
                rowCountTotal: {
                    operator: builderState.assertion?.volumeAssertion?.rowCountTotal?.operator as AssertionStdOperator,
                    parameters: builderState.assertion?.volumeAssertion?.parameters as AssertionStdParameters,
                },
            };
        case VolumeAssertionType.RowCountChange:
            return {
                rowCountChange: {
                    type: builderState.assertion?.volumeAssertion?.rowCountChange?.type as AssertionValueChangeType,
                    operator: builderState.assertion?.volumeAssertion?.rowCountChange?.operator as AssertionStdOperator,
                    parameters: builderState.assertion?.volumeAssertion?.parameters as AssertionStdParameters,
                },
            };
        case VolumeAssertionType.IncrementingSegmentRowCountTotal:
            return {
                incrementingSegmentRowCountTotal: {
                    segment: builderState.assertion?.volumeAssertion?.segment as IncrementingSegmentSpecInput,
                    operator: builderState.assertion?.volumeAssertion?.incrementingSegmentRowCountTotal
                        ?.operator as AssertionStdOperator,
                    parameters: builderState.assertion?.volumeAssertion?.parameters as AssertionStdParameters,
                },
            };
        case VolumeAssertionType.IncrementingSegmentRowCountChange:
            return {
                incrementingSegmentRowCountChange: {
                    segment: builderState.assertion?.volumeAssertion?.segment as IncrementingSegmentSpecInput,
                    type: builderState.assertion?.volumeAssertion?.incrementingSegmentRowCountChange
                        ?.type as AssertionValueChangeType,
                    operator: builderState.assertion?.volumeAssertion?.incrementingSegmentRowCountChange
                        ?.operator as AssertionStdOperator,
                    parameters: builderState.assertion?.volumeAssertion?.parameters as AssertionStdParameters,
                },
            };
        default:
            return undefined;
    }
};

export const builderStateToUpdateVolumeAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    const volumeTypeVariables = builderStateToVolumeTypeAssertionVariables(builderState);

    return {
        input: {
            type: builderState.assertion?.volumeAssertion?.type as VolumeAssertionType,
            filter: builderState.assertion?.volumeAssertion?.filter
                ? {
                      type: builderState.assertion?.volumeAssertion?.filter.type as DatasetFilterType,
                      sql: builderState.assertion?.volumeAssertion?.filter.sql,
                  }
                : undefined,
            actions: builderState.assertion?.actions
                ? {
                      onSuccess: builderState.assertion?.actions?.onSuccess || [],
                      onFailure: builderState.assertion?.actions?.onFailure || [],
                  }
                : undefined,
            ...volumeTypeVariables,
        },
    };
};

export const builderStateToUpdateSqlAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return {
        input: {
            type: builderState.assertion?.sqlAssertion?.type as SqlAssertionType,
            description: builderState.assertion?.description,
            statement: builderState.assertion?.sqlAssertion?.statement,
            changeType: builderState.assertion?.sqlAssertion?.changeType as AssertionValueChangeType,
            operator: builderState.assertion?.sqlAssertion?.operator as AssertionStdOperator,
            parameters:
                builderState.assertion?.sqlAssertion?.operator === AssertionStdOperator.Between
                    ? {
                          minValue: builderState.assertion.sqlAssertion.parameters?.minValue,
                          maxValue: builderState.assertion.sqlAssertion.parameters?.maxValue,
                      }
                    : {
                          value: builderState.assertion?.sqlAssertion?.parameters?.value,
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

export const builderStateToUpdateFieldAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return {
        input: {
            type: builderState.assertion?.fieldAssertion?.type as FieldAssertionType,
            fieldValuesAssertion:
                builderState.assertion?.fieldAssertion?.type === FieldAssertionType.FieldValues
                    ? builderState.assertion?.fieldAssertion?.fieldValuesAssertion
                    : undefined,
            fieldMetricAssertion:
                builderState.assertion?.fieldAssertion?.type === FieldAssertionType.FieldMetric
                    ? builderState.assertion?.fieldAssertion?.fieldMetricAssertion
                    : undefined,
            filter: builderState.assertion?.fieldAssertion?.filter
                ? {
                      type: builderState.assertion?.fieldAssertion?.filter.type as DatasetFilterType,
                      sql: builderState.assertion?.fieldAssertion?.filter.sql,
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
            executorId: builderState.executorId,
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

export const builderStateToCreateVolumeAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return {
        input: {
            entityUrn: builderState.entityUrn as string,
            ...builderStateToUpdateVolumeAssertionVariables(builderState).input,
        },
    };
};

export const builderStateToCreateSqlAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return {
        input: {
            entityUrn: builderState.entityUrn as string,
            ...builderStateToUpdateSqlAssertionVariables(builderState).input,
        },
    };
};

export const builderStateToCreateFieldAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return {
        input: {
            entityUrn: builderState.entityUrn as string,
            ...builderStateToUpdateFieldAssertionVariables(builderState).input,
        },
    };
};

export const getAssertionTypesForEntityType = (entityType: EntityType) => {
    return ASSERTION_TYPES.filter((type) => type.entityTypes.includes(entityType));
};

export const getDefaultFreshnessSourceOption = (platformUrn: string, connectionForEntityExists: boolean) => {
    if (!connectionForEntityExists) {
        return DatasetFreshnessSourceType.DatahubOperation;
    }
    return PLATFORM_ASSERTION_CONFIGS[platformUrn]?.freshness.defaultSourceType || DatasetFreshnessSourceType.AuditLog;
};

export const getDefaultDatasetFreshnessAssertionParametersState = (
    platformUrn: string,
    connectionForEntityExists: boolean,
) => {
    return {
        type: AssertionEvaluationParametersType.DatasetFreshness,
        datasetFreshnessParameters: {
            sourceType: getDefaultFreshnessSourceOption(platformUrn, connectionForEntityExists),
            auditLog: {},
        },
    };
};

export const getFreshnessSourceOptions = (platformUrn: string, connectionForEntityExists: boolean) => {
    const allowedSourceTypes = connectionForEntityExists
        ? PLATFORM_ASSERTION_CONFIGS[platformUrn].freshness.sourceTypes
        : [DatasetFreshnessSourceType.DatahubOperation];
    return allSourceOptions.filter((option) => allowedSourceTypes.includes(option.type));
};

export const getFreshnessSourceOptionPlatformDescription = (platformUrn: string, type: DatasetFreshnessSourceType) => {
    return PLATFORM_ASSERTION_CONFIGS[platformUrn]?.freshness.sourceTypeDetails[type]?.description;
};

export const getFreshnessSourceOption = (type: DatasetFreshnessSourceType, kind?: Maybe<FreshnessFieldKind>) => {
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

/**
 * Used to identify fields that are of nested within a STRUCT. These fields are not eligible for use in Assertions.
 * In v1 paths, STRUCTs have 'dots' in the path (i.e. a.b.c.d)
 * In v2 paths, STRUCTs have 'type=struct' in the path (i.e. [type=Struct])
 */
export const isStructField = (field: SchemaField) => {
    return field.fieldPath.includes('type=struct') || field.fieldPath.includes('.');
};
