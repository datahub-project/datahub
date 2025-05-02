import { Maybe } from 'graphql/jsutils/Maybe';
import { keyBy } from 'lodash';
import React from 'react';

import {
    ASSERTION_TYPES,
    HIGH_WATERMARK_FIELD_TYPES,
    LAST_MODIFIED_FIELD_TYPES,
} from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/constants';
import {
    AssertionActionsFormState,
    AssertionMonitorBuilderState,
} from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';
import { BIGQUERY_URN, DATABRICKS_URN, REDSHIFT_URN, SNOWFLAKE_URN } from '@app/ingest/source/builder/constants';
import { cleanAssertionDescription, removeNestedTypeNames } from '@app/shared/subscribe/drawer/utils';

import { UpdateAssertionMetadataMutationVariables } from '@graphql/assertion.generated';
import {
    Assertion,
    AssertionActionType,
    AssertionEvaluationParametersInput,
    AssertionEvaluationParametersType,
    AssertionStdOperator,
    AssertionStdParameters,
    AssertionType,
    AssertionValueChangeType,
    CreateFieldAssertionInput,
    CreateFreshnessAssertionInput,
    CreateSchemaAssertionInput,
    CreateSqlAssertionInput,
    CreateVolumeAssertionInput,
    DataPlatform,
    DatasetFilterType,
    DatasetFreshnessSourceType,
    Entity,
    EntityType,
    FieldAssertionType,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
    FreshnessFieldKind,
    IncrementingSegmentSpecInput,
    Monitor,
    MonitorMode,
    SchemaField,
    SchemaFieldDataType,
    SqlAssertionType,
    VolumeAssertionType,
} from '@types';

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
            defaultSourceType: DatasetFreshnessSourceType.InformationSchema,
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
                            Edition (or higher) of Snowflake and is only supported for Tables, not Views. This View has
                            a latency of up to 180 minutes in Snowflake, so this is not recommended for high frequency
                            checks.
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
            defaultSourceType: DatasetFreshnessSourceType.InformationSchema,
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
    [DATABRICKS_URN]: {
        freshness: {
            defaultSourceType: DatasetFreshnessSourceType.AuditLog,
            sourceTypes: [
                DatasetFreshnessSourceType.AuditLog,
                DatasetFreshnessSourceType.InformationSchema,
                DatasetFreshnessSourceType.FieldValue,
                DatasetFreshnessSourceType.FileMetadata,
                DatasetFreshnessSourceType.DatahubOperation,
            ],
            sourceTypeDetails: {
                [DatasetFreshnessSourceType.AuditLog]: {
                    description: (
                        <>
                            We&apos;ll use Databricks{' '}
                            <b>
                                <a
                                    href="https://docs.databricks.com/en/delta/history.html"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                >
                                    Delta Lake Table History
                                </a>
                            </b>{' '}
                            to determine whether a Table has changed. <br />{' '}
                            <b>Note that this is only supported for tables stored in delta format.</b> Refer
                            `data_source_format` in properties to verify table&apos;s format. Table history retention is
                            determined by the table setting delta.logRetentionDuration, which is 30 days by default.
                        </>
                    ),
                },
                [DatasetFreshnessSourceType.InformationSchema]: {
                    // TODO: "Gray out" the options based on which format the current table is in.
                    description: (
                        <>
                            We&apos;ll use Databricks{' '}
                            <b>
                                <a
                                    href="https://docs.databricks.com/en/delta/table-details.html"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                >
                                    Delta Lake Describe Detail
                                </a>
                            </b>{' '}
                            query to determine whether the Table has changed. <br />{' '}
                            <b>Note that this is only supported for tables stored in delta format.</b> Refer
                            `data_source_format` in properties to verify table&apos;s format.
                        </>
                    ),
                },
                [DatasetFreshnessSourceType.FieldValue]: {
                    description: (
                        <>
                            We&apos;ll query a specific column of the Databricks Table or View to determine whether it
                            has changed.
                            <br /> This requires that the configured service principal (token) has read access to the
                            asset.
                        </>
                    ),
                },
                [DatasetFreshnessSourceType.FileMetadata]: {
                    description: (
                        <>
                            We&apos;ll use Databricks{' '}
                            <b>
                                <a
                                    href="https://docs.databricks.com/en/ingestion/file-metadata-column.html"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                >
                                    File metadata column
                                </a>
                            </b>{' '}
                            to determine whether the Table has changed. This requires that the configured service
                            principal (token) has read access to the asset. This is supported for managed as well as
                            external tables in Unity Catalog and Hive Metastore. <br />
                            <b>
                                As of now, this is not supported for tables created with{' '}
                                <a
                                    href="https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-hiveformat.html"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                >
                                    hive format
                                </a>
                            </b>
                            . Refer `data_source_format` in properties to verify table&apos;s format.
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
        allowedScheduleTypes: [
            FreshnessAssertionScheduleType.FixedInterval,
            FreshnessAssertionScheduleType.Cron,
            FreshnessAssertionScheduleType.SinceTheLastCheck,
        ],
    },
    {
        type: DatasetFreshnessSourceType.InformationSchema,
        name: 'Information Schema',
        description: 'Use the information schema or system metadata tables to determine whether the asset has changed',
        allowedScheduleTypes: [
            FreshnessAssertionScheduleType.FixedInterval,
            FreshnessAssertionScheduleType.Cron,
            FreshnessAssertionScheduleType.SinceTheLastCheck,
        ],
    },
    {
        type: DatasetFreshnessSourceType.FileMetadata,
        name: 'File Metadata',
        description: "Use the underlying file system's metadata to determine whether the asset has changed",
        allowedScheduleTypes: [
            FreshnessAssertionScheduleType.FixedInterval,
            FreshnessAssertionScheduleType.Cron,
            FreshnessAssertionScheduleType.SinceTheLastCheck,
        ],
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
        allowedScheduleTypes: [
            FreshnessAssertionScheduleType.FixedInterval,
            FreshnessAssertionScheduleType.Cron,
            FreshnessAssertionScheduleType.SinceTheLastCheck,
        ],
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
        allowedScheduleTypes: [FreshnessAssertionScheduleType.Cron, FreshnessAssertionScheduleType.SinceTheLastCheck],
    },
    {
        type: DatasetFreshnessSourceType.DatahubOperation,
        name: 'DataHub Operation',
        description:
            'Use the DataHub "Operation" Aspect to determine whether the table has changed. This avoids the requirement to contact your data platform to determine evaluate Freshness Assertions. Note that this relies on operations being reported to DataHub, either via ingestion or via use of the DataHub APIs (reportOperation).',
        allowedScheduleTypes: [
            FreshnessAssertionScheduleType.FixedInterval,
            FreshnessAssertionScheduleType.Cron,
            FreshnessAssertionScheduleType.SinceTheLastCheck,
        ],
    },
];

/** Create a unique identifier for each source config option */
const getSourceOptionKey = (type: DatasetFreshnessSourceType, kind?: Maybe<FreshnessFieldKind>) => {
    return `${type}.${kind || ''}`;
};
/** Map of all source options to allow constant lookup by Source Type and Field Kind */
const sourceOptionsByKey = keyBy(allSourceOptions, ({ type, field }) => getSourceOptionKey(type, field?.kind));

export const builderStateToSharedFreshnessAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return removeNestedTypeNames({
        description: builderState.assertion?.description,
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
    });
};

export const builderStateToUpsertFreshnessAssertionMonitorVariables = (builderState: AssertionMonitorBuilderState) => {
    return removeNestedTypeNames({
        assertionUrn: builderState?.assertion?.urn,
        input: {
            ...builderStateToSharedFreshnessAssertionVariables(builderState),
            // Monitor parameters
            evaluationSchedule: builderState.schedule,
            evaluationParameters: builderState.parameters?.datasetFreshnessParameters,
            mode: MonitorMode.Active,
            entityUrn: builderState.entityUrn,
        },
    });
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

export const builderStateToSharedVolumeAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    const volumeTypeVariables = builderStateToVolumeTypeAssertionVariables(builderState);
    return removeNestedTypeNames({
        type: builderState.assertion?.volumeAssertion?.type as VolumeAssertionType,
        description: builderState.assertion?.description,
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
    });
};

export const builderStateToUpsertVolumeAssertionMonitorVariables = (builderState: AssertionMonitorBuilderState) => {
    return removeNestedTypeNames({
        assertionUrn: builderState?.assertion?.urn,
        input: {
            ...builderStateToSharedVolumeAssertionVariables(builderState),
            // Monitor parameters
            evaluationSchedule: builderState.schedule,
            evaluationParameters: builderState.parameters?.datasetVolumeParameters,
            mode: MonitorMode.Active,
            entityUrn: builderState.entityUrn,
        },
    });
};

export const builderStateToSharedSqlAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return removeNestedTypeNames({
        type: builderState.assertion?.sqlAssertion?.type as SqlAssertionType,
        description: builderState.assertion?.description,
        statement: builderState.assertion?.sqlAssertion?.statement,
        changeType: builderState.assertion?.sqlAssertion?.changeType as AssertionValueChangeType,
        operator: builderState.assertion?.sqlAssertion?.operator as AssertionStdOperator,
        parameters:
            builderState.assertion?.sqlAssertion?.operator === AssertionStdOperator.Between
                ? {
                      minValue: {
                          type: builderState.assertion?.sqlAssertion?.parameters?.minValue?.type,
                          value: builderState.assertion?.sqlAssertion?.parameters?.minValue?.value,
                      },
                      maxValue: {
                          type: builderState?.assertion?.sqlAssertion?.parameters?.maxValue?.type,
                          value: builderState?.assertion?.sqlAssertion?.parameters?.maxValue?.value,
                      },
                  }
                : {
                      value: {
                          type: builderState?.assertion?.sqlAssertion?.parameters?.value?.type,
                          value: builderState?.assertion?.sqlAssertion?.parameters?.value?.value,
                      },
                  },
        actions: builderState.assertion?.actions
            ? {
                  onSuccess: builderState.assertion?.actions?.onSuccess || [],
                  onFailure: builderState.assertion?.actions?.onFailure || [],
              }
            : undefined,
    });
};

export const builderStateToUpsertSqlAssertionMonitorVariables = (builderState: AssertionMonitorBuilderState) => {
    return removeNestedTypeNames({
        assertionUrn: builderState?.assertion?.urn,
        input: {
            ...builderStateToSharedSqlAssertionVariables(builderState),
            // Monitor parameters
            evaluationSchedule: builderState.schedule,
            mode: MonitorMode.Active,
            entityUrn: builderState.entityUrn,
        },
    });
};

export const builderStateToSharedFieldAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return removeNestedTypeNames({
        type: builderState.assertion?.fieldAssertion?.type as FieldAssertionType,
        description: builderState.assertion?.description,
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
    });
};

export const builderStateToUpsertFieldAssertionMonitorVariables = (builderState: AssertionMonitorBuilderState) => {
    return removeNestedTypeNames({
        assertionUrn: builderState?.assertion?.urn,
        input: {
            ...builderStateToSharedFieldAssertionVariables(builderState),
            // Monitor parameters
            evaluationSchedule: builderState.schedule,
            evaluationParameters: builderState.parameters?.datasetFieldParameters,
            mode: MonitorMode.Active,
            entityUrn: builderState.entityUrn,
        },
    });
};

export const builderStateToUpsertSchemaAssertionMonitorVariables = (builderState: AssertionMonitorBuilderState) => {
    return removeNestedTypeNames({
        assertionUrn: builderState?.assertion?.urn,
        input: {
            assertion: {
                compatibility: builderState?.assertion?.schemaAssertion?.compatibility,
                fields: builderState?.assertion?.schemaAssertion?.fields,
            },
            description: builderState.assertion?.description,
            mode: MonitorMode.Active,
            entityUrn: builderState.entityUrn,
            actions: builderState.assertion?.actions
                ? {
                      onSuccess: builderState.assertion?.actions?.onSuccess || [],
                      onFailure: builderState.assertion?.actions?.onFailure || [],
                  }
                : undefined,
            evaluationSchedule: builderState.schedule,
        },
    });
};

export const builderStateToTestFreshnessAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return {
        input: {
            entityUrn: builderState.entityUrn as string,
            ...builderStateToSharedFreshnessAssertionVariables(builderState),
            type: FreshnessAssertionType.DatasetChange,
        },
    };
};

export const builderStateToTestVolumeAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return {
        input: {
            entityUrn: builderState.entityUrn as string,
            ...builderStateToSharedVolumeAssertionVariables(builderState),
        },
    };
};

export const builderStateToTestSqlAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return {
        input: {
            entityUrn: builderState.entityUrn as string,
            ...builderStateToSharedSqlAssertionVariables(builderState),
        },
    };
};

export const builderStateToTestFieldAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return {
        input: {
            entityUrn: builderState.entityUrn as string,
            ...builderStateToSharedFieldAssertionVariables(builderState),
        },
    };
};

export const builderStateToTestSchemaAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return removeNestedTypeNames({
        entityUrn: builderState.entityUrn,
        compatibility: builderState?.assertion?.schemaAssertion?.compatibility,
        fields: builderState?.assertion?.schemaAssertion?.fields,
    });
};

export const getAssertionTypesForEntityType = (entityType: EntityType) => {
    return ASSERTION_TYPES.filter((type) => type.entityTypes.includes(entityType));
};

export const getDefaultFreshnessSourceOption = (
    platformUrn: string,
    monitorsConnectionForEntityExists: boolean,
): DatasetFreshnessSourceType => {
    if (!monitorsConnectionForEntityExists) {
        return DatasetFreshnessSourceType.DatahubOperation;
    }
    return PLATFORM_ASSERTION_CONFIGS[platformUrn]?.freshness?.defaultSourceType || DatasetFreshnessSourceType.AuditLog;
};

export const getDefaultDatasetFreshnessAssertionParametersState = (
    platformUrn: string,
    monitorsConnectionForEntityExists: boolean,
) => {
    return {
        type: AssertionEvaluationParametersType.DatasetFreshness,
        datasetFreshnessParameters: {
            sourceType: getDefaultFreshnessSourceOption(platformUrn, monitorsConnectionForEntityExists),
            auditLog: {},
        },
    };
};

export const getFreshnessSourceOptions = (platformUrn: string, connectionForEntityExists: boolean) => {
    const allowedSourceTypes: DatasetFreshnessSourceType[] | undefined = connectionForEntityExists
        ? PLATFORM_ASSERTION_CONFIGS[platformUrn]?.freshness?.sourceTypes
        : [DatasetFreshnessSourceType.DatahubOperation];
    return allSourceOptions.filter((option) => allowedSourceTypes?.includes(option.type));
};

export const getFreshnessSourceOptionPlatformDescription = (platformUrn: string, type: DatasetFreshnessSourceType) => {
    return PLATFORM_ASSERTION_CONFIGS[platformUrn]?.freshness?.sourceTypeDetails[type]?.description;
};

export const getFreshnessSourceOption = (type: DatasetFreshnessSourceType, kind?: Maybe<FreshnessFieldKind>) => {
    return sourceOptionsByKey[getSourceOptionKey(type, kind)];
};

/**
 * Returns true if the entity is eligible for online assertion monitoring.
 * Currently limited to Snowflake, Redshift, and BigQuery.
 */
const ASSERTION_SUPPORTED_PLATFORM_URNS = [SNOWFLAKE_URN, REDSHIFT_URN, BIGQUERY_URN, DATABRICKS_URN];
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

export const builderStateToUpdateAssertionMetadataVariables = (
    builderState: AssertionMonitorBuilderState,
): UpdateAssertionMetadataMutationVariables | undefined => {
    return builderState.assertion?.actions && builderState.assertion?.urn
        ? {
              urn: builderState.assertion.urn,
              input: {
                  description: builderState.assertion.description,
                  actions: {
                      onSuccess: builderState.assertion.actions.onSuccess || [],
                      onFailure: builderState.assertion.actions.onFailure || [],
                  },
              },
          }
        : undefined;
};

/**
 * Used to identify fields that are of nested within a STRUCT. These fields are not eligible for use in Assertions.
 * In v1 paths, STRUCTs have 'dots' in the path (i.e. a.b.c.d)
 * In v2 paths, STRUCTs have 'type=struct' in the path (i.e. [type=Struct])
 */
export const isStructField = (field: SchemaField) => {
    return field.fieldPath.includes('type=struct') || field.fieldPath.includes('.');
};

const convertAssertionToBuilderState = (assertion: Assertion): AssertionMonitorBuilderState['assertion'] => {
    return {
        urn: assertion?.urn,
        type: assertion?.info?.type,
        description: assertion?.info?.description,
        actions: {
            onSuccess: assertion.actions?.onSuccess?.map((action) => ({ type: action.type })) || [],
            onFailure: assertion.actions?.onFailure?.map((action) => ({ type: action.type })) || [],
        },
        freshnessAssertion: {
            schedule: assertion.info?.freshnessAssertion?.schedule,
            filter: assertion.info?.freshnessAssertion?.filter,
        },
        volumeAssertion: {
            type: assertion.info?.volumeAssertion?.type,
            rowCountTotal: assertion.info?.volumeAssertion?.rowCountTotal,
            rowCountChange: assertion.info?.volumeAssertion?.rowCountChange,
            incrementingSegmentRowCountTotal: assertion.info?.volumeAssertion?.incrementingSegmentRowCountTotal,
            incrementingSegmentRowCountChange: assertion.info?.volumeAssertion?.incrementingSegmentRowCountChange,
            // This is a divergence in the model.
            parameters:
                assertion.info?.volumeAssertion?.rowCountTotal?.parameters ||
                assertion.info?.volumeAssertion?.rowCountChange?.parameters ||
                undefined,
            filter: assertion.info?.volumeAssertion?.filter,
        },
        sqlAssertion: {
            type: assertion.info?.sqlAssertion?.type,
            statement: assertion.info?.sqlAssertion?.statement,
            changeType: assertion.info?.sqlAssertion?.changeType,
            operator: assertion.info?.sqlAssertion?.operator,
            parameters: assertion.info?.sqlAssertion?.parameters,
        },
        fieldAssertion: {
            type: assertion.info?.fieldAssertion?.type,
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore  NOTE: this is type `FieldValuesAssertion` but `AssertionMonitorBuilderState` has hardcoded every individual field so we'll have to manually map it
            fieldValuesAssertion: assertion.info?.fieldAssertion?.fieldValuesAssertion,
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore TODO(@jjoyce0510): can we convert these fields on `AssertionMonitorBuilderState` to just use the generated types instead of manually defining every prop?
            fieldMetricAssertion: assertion.info?.fieldAssertion?.fieldMetricAssertion,
            filter: assertion.info?.fieldAssertion?.filter,
        },
        schemaAssertion: {
            compatibility: assertion.info?.schemaAssertion?.compatibility,
            fields: assertion.info?.schemaAssertion?.fields,
        },
    };
};

export const createAssertionMonitorBuilderState = (
    assertion: Assertion,
    entity: Entity & { platform?: DataPlatform },
    monitor?: Monitor,
): AssertionMonitorBuilderState => {
    return {
        entityUrn: entity.urn,
        platformUrn: entity.platform?.urn,
        assertion: convertAssertionToBuilderState(assertion),
        schedule: monitor?.info?.assertionMonitor?.assertions?.[0]?.schedule,
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore TODO(@jjoyce0510): same as l784
        parameters: monitor?.info?.assertionMonitor?.assertions?.[0]?.parameters,
    };
};

export const getAssertionInput = (builderStateData, urn: string) => {
    const { type, newBuilderStateData } = cleanAssertionDescription(builderStateData);

    switch (type) {
        case AssertionType.Field:
            return {
                type,
                connectionUrn: urn,
                fieldTestInput: builderStateToTestFieldAssertionVariables(newBuilderStateData)
                    .input as CreateFieldAssertionInput,
                parameters: removeNestedTypeNames(
                    newBuilderStateData?.parameters,
                ) as AssertionEvaluationParametersInput,
            };
        case AssertionType.Freshness:
            return {
                type,
                connectionUrn: urn,
                freshnessTestInput: builderStateToTestFreshnessAssertionVariables(newBuilderStateData)
                    .input as CreateFreshnessAssertionInput,
                parameters: removeNestedTypeNames(
                    newBuilderStateData?.parameters,
                ) as AssertionEvaluationParametersInput,
            };
        case AssertionType.Volume:
            return {
                type,
                connectionUrn: urn,
                volumeTestInput: builderStateToTestVolumeAssertionVariables(newBuilderStateData)
                    .input as CreateVolumeAssertionInput,
                parameters: removeNestedTypeNames(
                    newBuilderStateData?.parameters,
                ) as AssertionEvaluationParametersInput,
            };
        case AssertionType.Sql:
            return {
                type,
                connectionUrn: urn,
                sqlTestInput: builderStateToTestSqlAssertionVariables(newBuilderStateData)
                    .input as CreateSqlAssertionInput,
            };
        case AssertionType.DataSchema:
            return {
                type,
                connectionUrn: urn,
                schemaTestInput: builderStateToTestSchemaAssertionVariables(
                    newBuilderStateData,
                ) as CreateSchemaAssertionInput,
                parameters: removeNestedTypeNames(newBuilderStateData.parameters) as AssertionEvaluationParametersInput,
            };
        default:
            return {
                type,
                connectionUrn: urn,
            };
    }
};
