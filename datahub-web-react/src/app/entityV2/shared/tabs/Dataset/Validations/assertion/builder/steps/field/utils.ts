import { downgradeV2FieldPath } from '@app/entityV2/dataset/profile/schema/utils/utils';
import { HIGH_WATERMARK_FIELD_TYPES } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/constants';
import {
    SECTION_LABELS,
    groupOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/common/groupedOptions';
import {
    AssertionMonitorBuilderState,
    FieldMetricAssertionBuilderOperator,
    FieldMetricAssertionBuilderOperatorOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import {
    isEntityEligibleForAssertionMonitoring,
    isStructField,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/utils';
import { nullsToUndefined } from '@src/app/entityV2/shared/utils';

import {
    Assertion,
    AssertionEvaluationParametersType,
    AssertionStdOperator,
    AssertionStdParameterType,
    AssertionType,
    DatasetFieldAssertionSourceType,
    DatasetSchemaSourceType,
    FieldAssertionType,
    FieldMetricType,
    FieldTransformType,
    FieldValuesFailThresholdType,
    FreshnessFieldKind,
    Monitor,
    SchemaAssertionCompatibility,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
} from '@types';

export const getFieldAssertionTypeKey = (fieldAssertionType?: FieldAssertionType | null) => {
    switch (fieldAssertionType) {
        case FieldAssertionType.FieldValues:
            return 'fieldValuesAssertion';
        case FieldAssertionType.FieldMetric:
            return 'fieldMetricAssertion';
        default:
            throw new Error(`Unknown field assertion type: ${fieldAssertionType}`);
    }
};

export type EligibleFieldColumn = {
    path: string;
    type: SchemaFieldDataType;
    nativeType: string;
    kind?: FreshnessFieldKind;
};

export const getEligibleFieldColumns = (fields: SchemaField[]): EligibleFieldColumn[] => {
    // Keep allowedColumnTypes in sync with ALLOWED_COLUMN_TYPES_FOR_SMART_COLUMN_METRIC_ASSERTION in acryl-cloud/src/acryl_datahub_cloud/sdk/assertion_input/smart_column_metric_assertion_input.py
    const allowedColumnTypes = [
        SchemaFieldDataType.String,
        SchemaFieldDataType.Number,
        SchemaFieldDataType.Boolean,
        SchemaFieldDataType.Date,
        SchemaFieldDataType.Time,
        SchemaFieldDataType.Null, // this is a workaround for an ingestion bug where Redshift fields are not properly typed
    ];
    const eligibleFields = fields.filter((f) => allowedColumnTypes.includes(f.type) && !isStructField(f));
    return eligibleFields.map((f) => ({
        path: f.fieldPath,
        type: f.type,
        nativeType: f.nativeDataType as string,
    }));
};

export const getEligibleChangedRowColumns = (fields: SchemaField[]) => {
    const eligibleFields = fields.filter((f) => HIGH_WATERMARK_FIELD_TYPES.has(f.type) && !isStructField(f));
    return eligibleFields.map((f) => ({
        path: f.fieldPath,
        type: f.type,
        nativeType: f.nativeDataType as string,
        kind: FreshnessFieldKind.HighWatermark,
    }));
};

export const FIELD_TYPE_CONFIG = {
    [FieldAssertionType.FieldMetric]: {
        label: 'Column Metric',
        value: FieldAssertionType.FieldMetric,
        description: 'Validate the column using common column metrics',
        requiresConnection: false,
    },
    [FieldAssertionType.FieldValues]: {
        label: 'Column Value',
        value: FieldAssertionType.FieldValues,
        description: 'Validate every row’s value for the column using custom conditions',
        requiresConnection: true,
    },
};

// Keep this in sync with FIELD_VALUES_OPERATOR_CONFIG in acryl-cloud/src/acryl_datahub_cloud/sdk/assertion_input/smart_column_metric_assertion_input.py
export const FIELD_VALUES_OPERATOR_CONFIG = {
    [SchemaFieldDataType.String]: [
        {
            label: 'Is null',
            value: AssertionStdOperator.Null,
            excludeNulls: false,
        },
        {
            label: 'Is not null',
            value: AssertionStdOperator.NotNull,
            excludeNulls: false,
        },
        {
            label: 'Is equal to',
            value: AssertionStdOperator.EqualTo,
            parameters: {
                value: {
                    type: AssertionStdParameterType.String,
                },
            },
        },
        {
            label: 'Is not equal to',
            value: AssertionStdOperator.NotEqualTo,
            parameters: {
                value: {
                    type: AssertionStdParameterType.String,
                },
            },
        },
        {
            label: 'Is in set',
            value: AssertionStdOperator.In,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Set,
                },
            },
        },
        {
            label: 'Is not in set',
            value: AssertionStdOperator.NotIn,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Set,
                },
            },
        },
        {
            label: 'Not empty',
            value: AssertionStdOperator.GreaterThanOrEqualTo,
            parameters: {
                value: {
                    value: '1',
                    type: AssertionStdParameterType.Number,
                },
            },
            transform: {
                type: FieldTransformType.Length,
            },
            hideParameters: true,
        },
        {
            label: 'Matches regex',
            value: AssertionStdOperator.RegexMatch,
            parameters: {
                value: {
                    type: AssertionStdParameterType.String,
                },
            },
        },
        {
            label: 'Length is greater than',
            value: AssertionStdOperator.GreaterThan,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Number,
                },
            },
            transform: {
                type: FieldTransformType.Length,
            },
            inputType: 'number',
        },
        {
            label: 'Length is less than',
            value: AssertionStdOperator.LessThan,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Number,
                },
            },
            transform: {
                type: FieldTransformType.Length,
            },
            inputType: 'number',
        },
        {
            label: 'Length is in range',
            value: AssertionStdOperator.Between,
            parameters: {
                minValue: {
                    type: AssertionStdParameterType.Number,
                },
                maxValue: {
                    type: AssertionStdParameterType.Number,
                },
            },
            transform: {
                type: FieldTransformType.Length,
            },
        },
    ],
    [SchemaFieldDataType.Number]: [
        {
            label: 'Is greater than',
            value: AssertionStdOperator.GreaterThan,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Number,
                },
            },
            inputType: 'number',
        },
        {
            label: 'Is less than',
            value: AssertionStdOperator.LessThan,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Number,
                },
            },
            inputType: 'number',
        },
        {
            label: 'Is in range',
            value: AssertionStdOperator.Between,
            parameters: {
                minValue: {
                    type: AssertionStdParameterType.Number,
                },
                maxValue: {
                    type: AssertionStdParameterType.Number,
                },
            },
        },
        {
            label: 'Is null',
            value: AssertionStdOperator.Null,
            excludeNulls: false,
        },
        {
            label: 'Is not null',
            value: AssertionStdOperator.NotNull,
            excludeNulls: false,
        },
        {
            label: 'Is equal to',
            value: AssertionStdOperator.EqualTo,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Number,
                },
            },
            inputType: 'number',
        },
        {
            label: 'Is not equal to',
            value: AssertionStdOperator.NotEqualTo,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Number,
                },
            },
            inputType: 'number',
        },
        {
            label: 'Is in set',
            value: AssertionStdOperator.In,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Set,
                },
            },
            inputType: 'number',
        },
        {
            label: 'Is not in set',
            value: AssertionStdOperator.NotIn,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Set,
                },
            },
            inputType: 'number',
        },
        {
            label: 'Is not negative',
            value: AssertionStdOperator.GreaterThanOrEqualTo,
            parameters: {
                value: {
                    value: '0',
                    type: AssertionStdParameterType.Number,
                },
            },
            hideParameters: true,
        },
        {
            label: 'Is not zero',
            value: AssertionStdOperator.NotEqualTo,
            parameters: {
                value: {
                    value: '0',
                    type: AssertionStdParameterType.Number,
                },
            },
            hideParameters: true,
        },
    ],
    [SchemaFieldDataType.Boolean]: [
        {
            label: 'Is true',
            value: AssertionStdOperator.IsTrue,
        },
        {
            label: 'Is false',
            value: AssertionStdOperator.IsFalse,
        },
        {
            label: 'Is null',
            value: AssertionStdOperator.Null,
            excludeNulls: false,
        },
        {
            label: 'Is not null',
            value: AssertionStdOperator.NotNull,
            excludeNulls: false,
        },
    ],
    [SchemaFieldDataType.Date]: [
        {
            label: 'Is null',
            value: AssertionStdOperator.Null,
            excludeNulls: false,
        },
        {
            label: 'Is not null',
            value: AssertionStdOperator.NotNull,
            excludeNulls: false,
        },
    ],
    [SchemaFieldDataType.Time]: [
        {
            label: 'Is null',
            value: AssertionStdOperator.Null,
            excludeNulls: false,
        },
        {
            label: 'Is not null',
            value: AssertionStdOperator.NotNull,
            excludeNulls: false,
        },
    ],
    [SchemaFieldDataType.Null]: [
        {
            label: 'Is null',
            value: AssertionStdOperator.Null,
            excludeNulls: false,
        },
        {
            label: 'Is not null',
            value: AssertionStdOperator.NotNull,
            excludeNulls: false,
        },
    ],
};

type FieldMetric = {
    value: FieldMetricType;
    label: string;
    description: string;
    requiresConnection?: boolean;
};

// Keep this in sync with FIELD_METRIC_TYPE_CONFIG in acryl-cloud/src/acryl_datahub_cloud/sdk/assertion_input/smart_column_metric_assertion_input.py
export const FIELD_METRIC_TYPE_CONFIG: Partial<Record<SchemaFieldDataType, FieldMetric[]>> = {
    [SchemaFieldDataType.String]: [
        {
            label: 'Null count',
            value: FieldMetricType.NullCount,
            description: 'The number of rows where the column is null',
        },
        {
            label: 'Null percentage',
            value: FieldMetricType.NullPercentage,
            description: 'The percentage of rows where the column is null (0-100)',
        },
        {
            label: 'Unique count',
            value: FieldMetricType.UniqueCount,
            description: 'The number of unique values in the column',
        },
        {
            label: 'Unique percentage',
            value: FieldMetricType.UniquePercentage,
            description: 'The percentage of unique values in the column (0-100)',
        },
        {
            label: 'Max length',
            value: FieldMetricType.MaxLength,
            description: 'The maximum length of the string values in the column',
            requiresConnection: true,
        },
        {
            label: 'Min length',
            value: FieldMetricType.MinLength,
            description: 'The minimum length of the string values in the column',
            requiresConnection: true,
        },
        {
            label: 'Empty count',
            value: FieldMetricType.EmptyCount,
            description: 'The number of rows where the string is empty',
            requiresConnection: true,
        },
        {
            label: 'Empty percentage',
            value: FieldMetricType.EmptyPercentage,
            description: 'The percentage of rows where the string is empty (0-100)',
            requiresConnection: true,
        },
    ],
    [SchemaFieldDataType.Number]: [
        {
            label: 'Null count',
            value: FieldMetricType.NullCount,
            description: 'The number of rows where the column is null',
        },
        {
            label: 'Null percentage',
            value: FieldMetricType.NullPercentage,
            description: 'The percentage of rows where the column is null (0-100)',
        },
        {
            label: 'Unique count',
            value: FieldMetricType.UniqueCount,
            description: 'The number of unique values in the column',
        },
        {
            label: 'Unique percentage',
            value: FieldMetricType.UniquePercentage,
            description: 'The percentage of unique values in the column (0-100)',
        },
        {
            label: 'Max',
            value: FieldMetricType.Max,
            description: 'The maximum value in the column',
        },
        {
            label: 'Min',
            value: FieldMetricType.Min,
            description: 'The minimum value in the column',
        },
        {
            label: 'Average',
            value: FieldMetricType.Mean,
            description: 'The average value in the column',
        },
        {
            label: 'Median',
            value: FieldMetricType.Median,
            description: 'The median value in the column',
        },
        {
            label: 'Std dev',
            value: FieldMetricType.Stddev,
            description: 'The standard deviation of the values in the column',
        },
        {
            label: 'Negative count',
            value: FieldMetricType.NegativeCount,
            requiresConnection: true,
            description: 'The number of rows where the value is negative',
        },
        {
            label: 'Negative percentage',
            value: FieldMetricType.NegativePercentage,
            requiresConnection: true,
            description: 'The percentage of rows where the value is negative (0-100)',
        },
        {
            label: 'Zero count',
            value: FieldMetricType.ZeroCount,
            requiresConnection: true,
            description: 'The number of rows where the value is zero',
        },
        {
            label: 'Zero percentage',
            value: FieldMetricType.ZeroPercentage,
            requiresConnection: true,
            description: 'The percentage of rows where the value is zero (0-100)',
        },
    ],
    [SchemaFieldDataType.Boolean]: [
        {
            label: 'Null count',
            value: FieldMetricType.NullCount,
            description: 'The number of rows where the column is null',
        },
        {
            label: 'Null percentage',
            value: FieldMetricType.NullPercentage,
            description: 'The percentage of rows where the column is null (0-100)',
        },
        {
            label: 'Unique count',
            value: FieldMetricType.UniqueCount,
            description: 'The number of unique values in the column',
        },
        {
            label: 'Unique percentage',
            value: FieldMetricType.UniquePercentage,
            description: 'The percentage of unique values in the column (0-100)',
        },
    ],
    [SchemaFieldDataType.Date]: [
        {
            label: 'Null count',
            value: FieldMetricType.NullCount,
            description: 'The number of rows where the column is null',
        },
        {
            label: 'Null percentage',
            value: FieldMetricType.NullPercentage,
            description: 'The percentage of rows where the column is null (0-100)',
        },
        {
            label: 'Unique count',
            value: FieldMetricType.UniqueCount,
            description: 'The number of unique values in the column',
        },
        {
            label: 'Unique percentage',
            value: FieldMetricType.UniquePercentage,
            description: 'The percentage of unique values in the column (0-100)',
        },
    ],
    [SchemaFieldDataType.Time]: [
        {
            label: 'Null count',
            value: FieldMetricType.NullCount,
            description: 'The number of rows where the column is null',
        },
        {
            label: 'Null percentage',
            value: FieldMetricType.NullPercentage,
            description: 'The percentage of rows where the column is null (0-100)',
        },
        {
            label: 'Unique count',
            value: FieldMetricType.UniqueCount,
            description: 'The number of unique values in the column',
        },
        {
            label: 'Unique percentage',
            value: FieldMetricType.UniquePercentage,
            description: 'The percentage of unique values in the column (0-100)',
        },
    ],
    [SchemaFieldDataType.Null]: [
        {
            label: 'Null count',
            value: FieldMetricType.NullCount,
            description: 'The number of rows where the column is null',
        },
        {
            label: 'Null percentage',
            value: FieldMetricType.NullPercentage,
            description: 'The percentage of rows where the column is null (0-100)',
        },
        {
            label: 'Unique count',
            value: FieldMetricType.UniqueCount,
            description: 'The number of unique values in the column',
        },
        {
            label: 'Unique percentage',
            value: FieldMetricType.UniquePercentage,
            description: 'The percentage of unique values in the column (0-100)',
        },
    ],
};

export const getFieldTypeOptions = () => {
    return Object.values(FIELD_TYPE_CONFIG);
};

export const getFieldValuesOperatorOptions = (fieldType?: SchemaFieldDataType | null) => {
    if (!fieldType) return [];
    return FIELD_VALUES_OPERATOR_CONFIG[fieldType];
};

export const getSelectedFieldValuesOperatorOption = (
    fieldType?: SchemaFieldDataType | null,
    operator?: AssertionStdOperator | null,
) => {
    if (!fieldType || !operator) return null;
    return getFieldValuesOperatorOptions(fieldType).find((o) => o.value === operator);
};

export const getFieldMetricOperatorOptions = ({ disableAiInferred }: { disableAiInferred?: boolean } = {}) => {
    const options = getFieldValuesOperatorOptions(SchemaFieldDataType.Number).filter(
        (o) => ![AssertionStdOperator.Null, AssertionStdOperator.NotNull, AssertionStdOperator.In].includes(o.value),
    );
    if (disableAiInferred) {
        return options;
    }
    return [
        {
            label: 'Detect with AI',
            value: FieldMetricAssertionBuilderOperatorOptions.AiInferred,
            hideParameters: true,
        },
    ].concat(options);
};

// Grouped options for Select dropdown with section headers.
export const getFieldMetricOperatorOptionGroups = ({ disableAiInferred }: { disableAiInferred?: boolean } = {}) => {
    const baseOptions = getFieldMetricOperatorOptions({ disableAiInferred });
    const hasAi = !disableAiInferred;
    const aiOptions = hasAi
        ? baseOptions
              .filter((o) => o.value === FieldMetricAssertionBuilderOperatorOptions.AiInferred)
              .map((o) => ({ label: o.label, value: o.value }))
        : [];
    const valueOptions = baseOptions
        .filter((o) => o.value !== FieldMetricAssertionBuilderOperatorOptions.AiInferred)
        .map((o) => ({ label: o.label, value: o.value }));

    return groupOptions([
        [SECTION_LABELS.anomalyDetection, aiOptions],
        ['Metric Value', valueOptions],
    ]);
};
export const getSelectedFieldMetricOperatorOption = (operator?: FieldMetricAssertionBuilderOperator | null) => {
    if (!operator) return null;
    return getFieldMetricOperatorOptions().find((o) => o.value === operator);
};

export const getFieldMetricTypeOptions = (
    fieldType?: SchemaFieldDataType | null,
    sourceType?: DatasetFieldAssertionSourceType | null,
): FieldMetric[] => {
    if (!fieldType) return [];
    const isDatasetProfileSupported = sourceType !== DatasetFieldAssertionSourceType.DatahubDatasetProfile;
    return FIELD_METRIC_TYPE_CONFIG[fieldType]?.filter((o) => !o.requiresConnection || isDatasetProfileSupported) || [];
};

export const getSelectedFieldMetricTypeOption = (
    fieldType?: SchemaFieldDataType | null,
    metric?: FieldMetricType | null,
) => {
    if (!fieldType || !metric) return null;
    return getFieldMetricTypeOptions(fieldType).find((o) => o.value === metric);
};

export const getFieldMetricSourceTypeOptions = () => {
    return [
        {
            label: 'Query',
            description:
                'Calculates the field value every time the assertion runs by querying the dataset directly; most accurate, but may incur additional costs in the source system.',
            value: DatasetFieldAssertionSourceType.AllRowsQuery,
            requiresConnection: true,
        },
        {
            label: 'DataHub Dataset Profile',
            description:
                'Uses field statistics generated during DataHub profiling runs, avoiding additional source system costs. Requires profiling enabled for this dataset during ingestion and may be less reliable due to different scheduling cadences.',
            value: DatasetFieldAssertionSourceType.DatahubDatasetProfile,
            requiresConnection: false,
        },
    ];
};

// Default assertion definition used when the selected type is Field.
export const getDefaultDatasetFieldAssertionState = (connectionForEntityExists: boolean) => {
    return {
        // eslint-disable-next-line no-constant-condition
        type: connectionForEntityExists ? FieldAssertionType.FieldMetric : FieldAssertionType.FieldMetric,
        fieldValuesAssertion: {
            field: {},
            failThreshold: {
                type: FieldValuesFailThresholdType.Count,
                value: 0,
            },
            excludeNulls: true,
        },
        fieldMetricAssertion: {
            field: {},
        },
    };
};

// Default assertion definition used when the selected type is Data Schema.
export const getDefaultDatasetSchemaAssertionState = () => {
    return {
        compatibility: SchemaAssertionCompatibility.Superset,
        fields: [],
    };
};

// Default assertion parameter definition used when the selected type is Field.
export const getDefaultDatasetFieldAssertionParametersState = (connectionForEntityExists: boolean) => {
    return {
        type: AssertionEvaluationParametersType.DatasetField,
        datasetFieldParameters: {
            sourceType:
                // eslint-disable-next-line no-constant-condition
                connectionForEntityExists
                    ? DatasetFieldAssertionSourceType.AllRowsQuery
                    : DatasetFieldAssertionSourceType.DatahubDatasetProfile,
        },
    };
};

// Default assertion parameter definition used when the selected type is Data Schema
export const getDefaultDatasetSchemaAssertionParametersState = () => {
    return {
        type: AssertionEvaluationParametersType.DatasetSchema,
        datasetSchemaParameters: {
            sourceType: DatasetSchemaSourceType.DatahubSchema,
        },
    };
};

// Display a disabled message based on the dataset profile option depending on the platform and connection.
export const getDatasetProfileDisabledMessage = (
    platformUrn: string,
    sourceRequiresConnection: boolean,
    connectionForEntityExists: boolean,
) => {
    if (!sourceRequiresConnection) return null;
    if (!isEntityEligibleForAssertionMonitoring(platformUrn))
        return 'This option is not currently supported for this entity.';

    return !connectionForEntityExists
        ? 'This option is not currently supported for this entity. No connection found.'
        : null;
};

// Display a disabled message based on the selected metric option depending on the source type.
export const getInvalidMetricMessage = (
    sourceRequiresConnection: boolean,
    selectedMetricRequiresConnection?: boolean,
) => {
    return selectedMetricRequiresConnection && !sourceRequiresConnection
        ? 'This column metric condition is not supported when using DataHub Dataset Profile as the data source.'
        : null;
};

export const fieldAssertionToBuilderState = (
    assertion: Assertion,
    monitor: Monitor,
    entityUrn: string,
    platformUrn: string,
): AssertionMonitorBuilderState => {
    const fieldAssertion = assertion.info?.fieldAssertion as any;
    const parameters = monitor?.info?.assertionMonitor?.assertions?.[0]?.parameters;
    const datasetFieldParameters = parameters?.datasetFieldParameters;

    return {
        entityUrn,
        platformUrn,
        assertion: {
            type: AssertionType.Field,
            fieldAssertion,
        },
        parameters: {
            type: AssertionEvaluationParametersType.DatasetField,
            datasetFieldParameters: nullsToUndefined(datasetFieldParameters),
        },
    };
};

export const getFieldMetricLabel = (metric: FieldMetricType) => {
    const label = Object.values(FIELD_METRIC_TYPE_CONFIG)
        .flatMap((typeConfig) => typeConfig.filter((config) => config.value === metric))
        .map((filtered) => filtered.label)
        .find((l) => l !== undefined);

    return label;
};

export const convertSchemaMetadataToAssertionFields = (schemaMetadata: SchemaMetadata) => {
    return (
        schemaMetadata.fields?.map((field) => ({
            path: downgradeV2FieldPath(field.fieldPath) as string,
            type: field.type,
            nativeType: field.nativeDataType,
        })) || []
    );
};
