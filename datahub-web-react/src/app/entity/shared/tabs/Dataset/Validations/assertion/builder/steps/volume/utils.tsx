import React from 'react';

import { getIsRowCountChange, getVolumeTypeInfo } from '@app/entity/shared/tabs/Dataset/Validations/utils';
import { BIGQUERY_URN, DATABRICKS_URN, REDSHIFT_URN, SNOWFLAKE_URN } from '@app/ingest/source/builder/constants';

import {
    AssertionEvaluationParametersType,
    AssertionStdOperator,
    AssertionStdParameterType,
    DatasetVolumeSourceType,
    VolumeAssertionInfo,
    VolumeAssertionType,
} from '@types';

// Source type config
export type VolumeSourceType = {
    label: string;
    description: string;
};

export const VOLUME_SOURCE_TYPES: Record<DatasetVolumeSourceType, VolumeSourceType> = {
    [DatasetVolumeSourceType.InformationSchema]: {
        label: 'Information Schema',
        description: 'Use the information schema or system metadata tables to determine the row count',
    },
    [DatasetVolumeSourceType.Query]: {
        label: 'Query',
        description: 'Determine the table row count by issuing a COUNT(*) query',
    },
    [DatasetVolumeSourceType.DatahubDatasetProfile]: {
        label: 'DataHub Dataset Profile',
        description:
            'Use the DataHub Dataset Profile to determine the table row count. Note that this requires that dataset profiling statistics are up-to-date as of the assertion run time. Profiling settings for a given integration can be configured on the Ingestion page.',
    },
};

export const PLATFORM_ASSERTION_CONFIGS = {
    [SNOWFLAKE_URN]: {
        defaultSourceType: DatasetVolumeSourceType.InformationSchema,
        sourceTypes: [
            DatasetVolumeSourceType.InformationSchema,
            DatasetVolumeSourceType.Query,
            DatasetVolumeSourceType.DatahubDatasetProfile,
        ],
        sourceTypeDetails: {
            [DatasetVolumeSourceType.InformationSchema]: {
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
                        view to determine the row count. Note that this is only supported for Tables, not Views.
                    </>
                ),
            },
            [DatasetVolumeSourceType.Query]: {
                description: (
                    <>
                        We&apos;ll query the Snowflake Table or View to determine the row count. <br />
                        This requires that the configured user account has read access to the asset.
                    </>
                ),
            },
        },
    },
    [BIGQUERY_URN]: {
        defaultSourceType: DatasetVolumeSourceType.InformationSchema,
        sourceTypes: [
            DatasetVolumeSourceType.InformationSchema,
            DatasetVolumeSourceType.Query,
            DatasetVolumeSourceType.DatahubDatasetProfile,
        ],
        sourceTypeDetails: {
            [DatasetVolumeSourceType.InformationSchema]: {
                description: (
                    <>
                        We&apos;ll use BigQuery <b>System Tables (__TABLES__)</b> to determine the row count. This
                        requires that your configured Service Account has access to read data and metadata for the
                        Dataset (roles/bigquery.metadataViewer and roles/bigquery.dataViewer). <br /> This method is
                        only supported for Tables, not Views.
                    </>
                ),
            },
            [DatasetVolumeSourceType.Query]: {
                description: (
                    <>
                        We&apos;ll query the BigQuery Table or View to determine whether it has changed. <br />
                        This requires that the configured user account has read access to the asset.
                    </>
                ),
            },
        },
    },
    [REDSHIFT_URN]: {
        defaultSourceType: DatasetVolumeSourceType.InformationSchema,
        sourceTypes: [
            DatasetVolumeSourceType.InformationSchema,
            DatasetVolumeSourceType.Query,
            DatasetVolumeSourceType.DatahubDatasetProfile,
        ],
        sourceTypeDetails: {
            [DatasetVolumeSourceType.InformationSchema]: {
                description: (
                    <>
                        We&apos;ll use Redshift{' '}
                        <b>
                            <a
                                href="https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_TABLE_INFO.html"
                                target="_blank"
                                rel="noopener noreferrer"
                            >
                                SVV_TABLE_INFO Views
                            </a>
                        </b>{' '}
                        to determine whether the Table has changed.
                    </>
                ),
            },
            [DatasetVolumeSourceType.Query]: {
                description: (
                    <>
                        We&apos;ll query the Redshift Table or View to determine whether it has changed. <br />
                        This requires that the configured user account has read access to the asset.
                    </>
                ),
            },
        },
    },
    [DATABRICKS_URN]: {
        defaultSourceType: DatasetVolumeSourceType.Query,
        sourceTypes: [DatasetVolumeSourceType.Query, DatasetVolumeSourceType.DatahubDatasetProfile],
        sourceTypeDetails: {
            [DatasetVolumeSourceType.Query]: {
                description: (
                    <>
                        We&apos;ll query the Databricks Table or View to determine the row count. <br />
                        This requires that the configured service principal (token) has read access to the asset.
                    </>
                ),
            },
        },
    },
};

// Volume assertion type category config
export type VolumeTypeCategory = {
    label: string;
    disabled: boolean;
    getType: (hasSegment: boolean) => VolumeAssertionType;
};

export enum VolumeTypeCategoryEnum {
    ROW_COUNT = 'ROW_COUNT',
    GROWTH_RATE = 'GROWTH_RATE',
}

export const VOLUME_TYPE_CATEGORIES: Record<VolumeTypeCategoryEnum, VolumeTypeCategory> = {
    [VolumeTypeCategoryEnum.ROW_COUNT]: {
        label: 'Row Count',
        disabled: false,
        getType: (hasSegment: boolean) => {
            return hasSegment
                ? VolumeAssertionType.IncrementingSegmentRowCountTotal
                : VolumeAssertionType.RowCountTotal;
        },
    },
    [VolumeTypeCategoryEnum.GROWTH_RATE]: {
        label: 'Growth Rate',
        disabled: false,
        getType: (hasSegment: boolean) => {
            return hasSegment
                ? VolumeAssertionType.IncrementingSegmentRowCountChange
                : VolumeAssertionType.RowCountChange;
        },
    },
};

export const getVolumeTypeCategory = (categoryKey: VolumeTypeCategoryEnum) => {
    return VOLUME_TYPE_CATEGORIES[categoryKey];
};

export const getSelectedVolumeTypeCategory = (volumeAssertionType: VolumeAssertionType) => {
    switch (volumeAssertionType) {
        case VolumeAssertionType.RowCountTotal:
        case VolumeAssertionType.IncrementingSegmentRowCountTotal:
            return VolumeTypeCategoryEnum.ROW_COUNT;
        case VolumeAssertionType.RowCountChange:
        case VolumeAssertionType.IncrementingSegmentRowCountChange:
            return VolumeTypeCategoryEnum.GROWTH_RATE;
        default:
            throw new Error(`Unknown volume assertion type: ${volumeAssertionType}`);
    }
};

// Volume assertion type config
export type VolumeTypeOption = {
    label: string;
    operator: AssertionStdOperator;
    category: VolumeTypeCategoryEnum;
};

export enum VolumeTypeOptionEnum {
    LESS_THAN_ROWS = 'LESS_THAN_ROWS',
    GREATER_THAN_ROWS = 'GREATER_THAN_ROWS',
    ROWS_IN_RANGE = 'ROWS_IN_RANGE',
    GROWTH_LESS_THAN = 'GROWTH_LESS_THAN',
    GROWTH_GREATER_THAN = 'GROWTH_GREATER_THAN',
    GROWTH_IN_RANGE = 'GROWTH_IN_RANGE',
}

export const VOLUME_TYPE_OPTIONS: Record<VolumeTypeOptionEnum, VolumeTypeOption> = {
    [VolumeTypeOptionEnum.LESS_THAN_ROWS]: {
        label: 'Is less than or equal to',
        operator: AssertionStdOperator.LessThanOrEqualTo,
        category: VolumeTypeCategoryEnum.ROW_COUNT,
    },
    [VolumeTypeOptionEnum.GREATER_THAN_ROWS]: {
        label: 'Is greater than or equal to',
        operator: AssertionStdOperator.GreaterThanOrEqualTo,
        category: VolumeTypeCategoryEnum.ROW_COUNT,
    },
    [VolumeTypeOptionEnum.ROWS_IN_RANGE]: {
        label: 'Is within an expected range',
        operator: AssertionStdOperator.Between,
        category: VolumeTypeCategoryEnum.ROW_COUNT,
    },
    [VolumeTypeOptionEnum.GROWTH_LESS_THAN]: {
        label: 'Grows by at most',
        operator: AssertionStdOperator.LessThanOrEqualTo,
        category: VolumeTypeCategoryEnum.GROWTH_RATE,
    },
    [VolumeTypeOptionEnum.GROWTH_GREATER_THAN]: {
        label: 'Grows by at least',
        operator: AssertionStdOperator.GreaterThanOrEqualTo,
        category: VolumeTypeCategoryEnum.GROWTH_RATE,
    },
    [VolumeTypeOptionEnum.GROWTH_IN_RANGE]: {
        label: 'Is growing within an expected range',
        operator: AssertionStdOperator.Between,
        category: VolumeTypeCategoryEnum.GROWTH_RATE,
    },
};

export const VOLUME_TYPE_OPTIONS_BY_CATEGORY: Record<VolumeTypeCategoryEnum, VolumeTypeOptionEnum[]> = {
    [VolumeTypeCategoryEnum.ROW_COUNT]: Object.entries(VOLUME_TYPE_OPTIONS)
        .filter(([_, option]) => option.category === VolumeTypeCategoryEnum.ROW_COUNT)
        .map(([key, _]) => key as VolumeTypeOptionEnum),
    [VolumeTypeCategoryEnum.GROWTH_RATE]: Object.entries(VOLUME_TYPE_OPTIONS)
        .filter(([_, option]) => option.category === VolumeTypeCategoryEnum.GROWTH_RATE)
        .map(([key, _]) => key as VolumeTypeOptionEnum),
};

export const getVolumeTypeOptions = () => {
    return Object.entries(VOLUME_TYPE_OPTIONS_BY_CATEGORY).map(([categoryKey, categoryOptions]) => {
        const category = VOLUME_TYPE_CATEGORIES[categoryKey as VolumeTypeCategoryEnum];
        return {
            label: category.label,
            options: categoryOptions.map((optionKey) => {
                const option = VOLUME_TYPE_OPTIONS[optionKey as VolumeTypeOptionEnum];
                return {
                    label: option.label,
                    value: optionKey,
                    disabled: category.disabled,
                };
            }),
        };
    });
};

export const getVolumeTypeOption = (optionKey: VolumeTypeOptionEnum) => {
    return VOLUME_TYPE_OPTIONS[optionKey];
};

export const getSelectedVolumeTypeOption = (volumeAssertionInfo?: VolumeAssertionInfo) => {
    if (!volumeAssertionInfo) return undefined;

    const category = getSelectedVolumeTypeCategory(volumeAssertionInfo.type);
    const options = VOLUME_TYPE_OPTIONS_BY_CATEGORY[category];
    const typeInfo = getVolumeTypeInfo(volumeAssertionInfo);
    if (!typeInfo?.operator) return undefined;
    return options.find(
        (optionKey) =>
            VOLUME_TYPE_OPTIONS[optionKey].category === category &&
            VOLUME_TYPE_OPTIONS[optionKey].operator === typeInfo.operator,
    );
};

export const getParameterBuilderTitle = (type: VolumeAssertionType, operator: AssertionStdOperator) => {
    const isRelativeChange = getIsRowCountChange(type);

    switch (operator) {
        case AssertionStdOperator.LessThanOrEqualTo:
            return isRelativeChange ? 'Pass if this table grows by at most' : 'Pass if the table has at most';
        case AssertionStdOperator.GreaterThanOrEqualTo:
            return isRelativeChange ? 'Pass if this table grows by at least' : 'Pass if the table has at least';
        case AssertionStdOperator.Between:
            return isRelativeChange
                ? 'Pass if this table grows at least...'
                : 'Pass if this table row count is at least...';
        default:
            throw new Error(`Unknown operator: ${operator}`);
    }
};

export const getDefaultVolumeParameters = (operator: AssertionStdOperator) => {
    switch (operator) {
        case AssertionStdOperator.LessThanOrEqualTo:
        case AssertionStdOperator.GreaterThanOrEqualTo:
            return {
                value: {
                    type: AssertionStdParameterType.Number,
                    value: '1000',
                },
            };
        case AssertionStdOperator.Between:
            return {
                minValue: {
                    type: AssertionStdParameterType.Number,
                    value: '100',
                },
                maxValue: {
                    type: AssertionStdParameterType.Number,
                    value: '5000',
                },
            };
        default:
            throw new Error(`Unknown operator: ${operator}`);
    }
};

export const getVolumeSourceTypeOptions = (
    platformUrn: string,
    connectionForEntityExists: boolean,
): DatasetVolumeSourceType[] => {
    return connectionForEntityExists
        ? (PLATFORM_ASSERTION_CONFIGS[platformUrn]?.sourceTypes ?? [DatasetVolumeSourceType.DatahubDatasetProfile])
        : [DatasetVolumeSourceType.DatahubDatasetProfile];
};

export const getVolumeSourceTypeDetails = (
    platformUrn: string,
    sourceType: DatasetVolumeSourceType,
): { description: string } | undefined => {
    // TODO: type 'sourceTypeDetails', for now this function's type just returns {description} to fix a bug and get things rolling
    return PLATFORM_ASSERTION_CONFIGS[platformUrn]?.sourceTypeDetails[sourceType];
};

export const getDefaultVolumeSourceType = (platformUrn: string, connectionForEntityExists: boolean) => {
    return connectionForEntityExists
        ? (PLATFORM_ASSERTION_CONFIGS[platformUrn]?.defaultSourceType ?? DatasetVolumeSourceType.DatahubDatasetProfile)
        : DatasetVolumeSourceType.DatahubDatasetProfile;
};

export const getDefaultDatasetVolumeAssertionParametersState = (
    platformUrn: string,
    monitorsConnectionForEntityExists: boolean,
) => {
    return {
        type: AssertionEvaluationParametersType.DatasetVolume,
        datasetVolumeParameters: {
            sourceType: getDefaultVolumeSourceType(platformUrn, monitorsConnectionForEntityExists),
        },
    };
};
