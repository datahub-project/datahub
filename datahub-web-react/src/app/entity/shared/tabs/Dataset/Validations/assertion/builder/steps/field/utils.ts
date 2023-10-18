import {
    AssertionStdOperator,
    AssertionStdParameterType,
    FieldAssertionType,
    FieldTransformType,
    FreshnessFieldKind,
    SchemaField,
    SchemaFieldDataType,
} from '../../../../../../../../../../types.generated';
import { HIGH_WATERMARK_FIELD_TYPES } from '../../constants';
import { isStructField } from '../../utils';

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
        disabled: false,
    },
    [FieldAssertionType.FieldMetric]: {
        label: 'Column Metric',
        value: FieldAssertionType.FieldMetric,
        description: 'Validate the column using common column metrics',
        disabled: true,
    },
};

export const FIELD_VALUES_OPERATOR_CONFIG = {
    [SchemaFieldDataType.String]: [
        {
            label: 'Is null',
            value: AssertionStdOperator.NotNull,
            excludeNulls: false,
        },
        {
            label: 'Is not null',
            value: AssertionStdOperator.Null,
            excludeNulls: false,
        },
        {
            label: 'Is not equal to',
            value: AssertionStdOperator.EqualTo,
            parameters: {
                value: {
                    type: AssertionStdParameterType.String,
                },
            },
        },
        {
            label: 'Is not in set',
            value: AssertionStdOperator.In,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Set,
                },
            },
        },
        {
            label: 'Is empty',
            value: AssertionStdOperator.GreaterThan,
            parameters: {
                value: {
                    value: '0',
                    type: AssertionStdParameterType.Number,
                },
            },
            transform: {
                type: FieldTransformType.Length,
            },
            hideParameters: true,
        },
        {
            label: 'Does not match regex',
            value: AssertionStdOperator.RegexMatch,
            parameters: {
                value: {
                    type: AssertionStdParameterType.String,
                },
            },
        },
        {
            label: 'Length is greater than',
            value: AssertionStdOperator.LessThanOrEqualTo,
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
            value: AssertionStdOperator.GreaterThanOrEqualTo,
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
            label: 'Length is outside of range',
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
            value: AssertionStdOperator.LessThanOrEqualTo,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Number,
                },
            },
            inputType: 'number',
        },
        {
            label: 'Is less than',
            value: AssertionStdOperator.GreaterThanOrEqualTo,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Number,
                },
            },
            inputType: 'number',
        },
        {
            label: 'Is outside of range',
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
            value: AssertionStdOperator.NotNull,
            excludeNulls: false,
        },
        {
            label: 'Is not null',
            value: AssertionStdOperator.Null,
            excludeNulls: false,
        },
        {
            label: 'Is not equal to',
            value: AssertionStdOperator.EqualTo,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Number,
                },
            },
            inputType: 'number',
        },
        {
            label: 'Is not in set',
            value: AssertionStdOperator.In,
            parameters: {
                value: {
                    type: AssertionStdParameterType.Set,
                },
            },
            inputType: 'number',
        },
        {
            label: 'Is negative',
            value: AssertionStdOperator.GreaterThan,
            parameters: {
                value: {
                    value: '-1',
                    type: AssertionStdParameterType.Number,
                },
            },
            hideParameters: true,
        },
        {
            label: 'Is zero',
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
            value: AssertionStdOperator.IsFalse,
        },
        {
            label: 'Is false',
            value: AssertionStdOperator.IsTrue,
        },
        {
            label: 'Is null',
            value: AssertionStdOperator.NotNull,
            excludeNulls: false,
        },
        {
            label: 'Is not null',
            value: AssertionStdOperator.Null,
            excludeNulls: false,
        },
    ],
    [SchemaFieldDataType.Date]: [
        {
            label: 'Is null',
            value: AssertionStdOperator.NotNull,
            excludeNulls: false,
        },
        {
            label: 'Is not null',
            value: AssertionStdOperator.Null,
            excludeNulls: false,
        },
    ],
    [SchemaFieldDataType.Time]: [
        {
            label: 'Is null',
            value: AssertionStdOperator.NotNull,
            excludeNulls: false,
        },
        {
            label: 'Is not null',
            value: AssertionStdOperator.Null,
            excludeNulls: false,
        },
    ],
    [SchemaFieldDataType.Null]: [
        {
            label: 'Is null',
            value: AssertionStdOperator.NotNull,
            excludeNulls: false,
        },
        {
            label: 'Is not null',
            value: AssertionStdOperator.Null,
            excludeNulls: false,
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
