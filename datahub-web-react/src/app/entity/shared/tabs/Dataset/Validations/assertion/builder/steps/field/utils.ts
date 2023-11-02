import {
    Assertion,
    AssertionEvaluationParametersType,
    AssertionStdOperator,
    AssertionStdParameterType,
    AssertionType,
    DatasetFieldAssertionSourceType,
    FieldAssertionType,
    FieldMetricType,
    FieldTransformType,
    FieldValuesFailThresholdType,
    FreshnessFieldKind,
    Monitor,
    SchemaField,
    SchemaFieldDataType,
} from '../../../../../../../../../../types.generated';
import { HIGH_WATERMARK_FIELD_TYPES } from '../../constants';
import { AssertionMonitorBuilderState } from '../../types';
import { isEntityEligibleForAssertionMonitoring, isStructField } from '../../utils';

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

export const getEligibleFieldColumns = (fields: SchemaField[]) => {
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
    [FieldAssertionType.FieldValues]: {
        label: 'Column Value',
        value: FieldAssertionType.FieldValues,
        description: 'Validate every row’s value for the column using custom conditions',
        requiresConnection: true,
    },
    [FieldAssertionType.FieldMetric]: {
        label: 'Column Metric',
        value: FieldAssertionType.FieldMetric,
        description: 'Validate the column using common column metrics',
        requiresConnection: false,
    },
};

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
            label: 'Is in set',
            value: AssertionStdOperator.In,
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

export const FIELD_METRIC_TYPE_CONFIG = {
    [SchemaFieldDataType.String]: [
        {
            label: 'Null count',
            value: FieldMetricType.NullCount,
        },
        {
            label: 'Null percentage',
            value: FieldMetricType.NullPercentage,
        },
        {
            label: 'Unique count',
            value: FieldMetricType.UniqueCount,
        },
        {
            label: 'Unique percentage',
            value: FieldMetricType.UniquePercentage,
        },
        {
            label: 'Max length',
            value: FieldMetricType.MaxLength,
            requiresConnection: true,
        },
        {
            label: 'Min length',
            value: FieldMetricType.MinLength,
            requiresConnection: true,
        },
        {
            label: 'Empty count',
            value: FieldMetricType.EmptyCount,
            requiresConnection: true,
        },
        {
            label: 'Empty percentage',
            value: FieldMetricType.EmptyPercentage,
            requiresConnection: true,
        },
    ],
    [SchemaFieldDataType.Number]: [
        {
            label: 'Null count',
            value: FieldMetricType.NullCount,
        },
        {
            label: 'Null percentage',
            value: FieldMetricType.NullPercentage,
        },
        {
            label: 'Unique count',
            value: FieldMetricType.UniqueCount,
        },
        {
            label: 'Unique percentage',
            value: FieldMetricType.UniquePercentage,
        },
        {
            label: 'Max',
            value: FieldMetricType.Max,
        },
        {
            label: 'Min',
            value: FieldMetricType.Min,
        },
        {
            label: 'Average',
            value: FieldMetricType.Mean,
        },
        {
            label: 'Median',
            value: FieldMetricType.Median,
        },
        {
            label: 'Std dev',
            value: FieldMetricType.Stddev,
        },
        {
            label: 'Negative count',
            value: FieldMetricType.NegativeCount,
            requiresConnection: true,
        },
        {
            label: 'Negative percentage',
            value: FieldMetricType.NegativePercentage,
            requiresConnection: true,
        },
        {
            label: 'Zero count',
            value: FieldMetricType.ZeroCount,
            requiresConnection: true,
        },
        {
            label: 'Zero percentage',
            value: FieldMetricType.ZeroPercentage,
            requiresConnection: true,
        },
    ],
    [SchemaFieldDataType.Boolean]: [
        {
            label: 'Null count',
            value: FieldMetricType.NullCount,
        },
        {
            label: 'Null percentage',
            value: FieldMetricType.NullPercentage,
        },
        {
            label: 'Unique count',
            value: FieldMetricType.UniqueCount,
        },
        {
            label: 'Unique percentage',
            value: FieldMetricType.UniquePercentage,
        },
    ],
    [SchemaFieldDataType.Date]: [
        {
            label: 'Null count',
            value: FieldMetricType.NullCount,
        },
        {
            label: 'Null percentage',
            value: FieldMetricType.NullPercentage,
        },
        {
            label: 'Unique count',
            value: FieldMetricType.UniqueCount,
        },
        {
            label: 'Unique percentage',
            value: FieldMetricType.UniquePercentage,
        },
    ],
    [SchemaFieldDataType.Time]: [
        {
            label: 'Null count',
            value: FieldMetricType.NullCount,
        },
        {
            label: 'Null percentage',
            value: FieldMetricType.NullPercentage,
        },
        {
            label: 'Unique count',
            value: FieldMetricType.UniqueCount,
        },
        {
            label: 'Unique percentage',
            value: FieldMetricType.UniquePercentage,
        },
    ],
    [SchemaFieldDataType.Null]: [
        {
            label: 'Null count',
            value: FieldMetricType.NullCount,
        },
        {
            label: 'Null percentage',
            value: FieldMetricType.NullPercentage,
        },
        {
            label: 'Unique count',
            value: FieldMetricType.UniqueCount,
        },
        {
            label: 'Unique percentage',
            value: FieldMetricType.UniquePercentage,
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

export const getFieldMetricOperatorOptions = () => {
    return getFieldValuesOperatorOptions(SchemaFieldDataType.Number).filter(
        (o) => ![AssertionStdOperator.Null, AssertionStdOperator.NotNull, AssertionStdOperator.In].includes(o.value),
    );
};

export const getSelectedFieldMetricOperatorOption = (operator?: AssertionStdOperator | null) => {
    if (!operator) return null;
    return getFieldMetricOperatorOptions().find((o) => o.value === operator);
};

export const getFieldMetricTypeOptions = (
    fieldType?: SchemaFieldDataType | null,
    sourceType?: DatasetFieldAssertionSourceType | null,
) => {
    if (!fieldType) return [];
    const isDatasetProfileSupported = sourceType !== DatasetFieldAssertionSourceType.DatahubDatasetProfile;
    return FIELD_METRIC_TYPE_CONFIG[fieldType].filter((o) => !o.requiresConnection || isDatasetProfileSupported);
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
            description: 'Determine the metric value by issuing a query against the dataset',
            value: DatasetFieldAssertionSourceType.AllRowsQuery,
            requiresConnection: true,
        },
        {
            label: 'DataHub Dataset Profile',
            description:
                'Use the DataHub Dataset Profile to determine the metric value. Note that this requires that dataset profiling statistics are up-to-date as of the assertion run time. Profiling settings for a given integration can be configured on the Ingestion page.',
            value: DatasetFieldAssertionSourceType.DatahubDatasetProfile,
            requiresConnection: false,
        },
    ];
};

// Default assertion definition used when the selected type is Field.
export const getDefaultDatasetFieldAssertionState = (connectionForEntityExists: boolean) => {
    return {
        // eslint-disable-next-line no-constant-condition
        type: connectionForEntityExists ? FieldAssertionType.FieldValues : FieldAssertionType.FieldMetric,
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

// Display a disabled message based on the dataset profile option depending on the platform and connection.
export const getDatasetProfileDisabledMessage = (
    platformUrn: string,
    sourceRequiresConnection: boolean,
    connectionForEntityExists: boolean,
) => {
    if (!sourceRequiresConnection) return null;
    if (!isEntityEligibleForAssertionMonitoring(platformUrn))
        return 'This option is not currently supported for this entity. Not supported for this data platform.';

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
            datasetFieldParameters,
        },
    };
};
