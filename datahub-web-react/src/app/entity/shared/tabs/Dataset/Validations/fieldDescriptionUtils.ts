import {
    AssertionStdOperator,
    AssertionStdParameters,
    FieldAssertionInfo,
    FieldAssertionType,
    FieldMetricType,
    FieldTransformType,
} from '../../../../../../types.generated';
import { formatNumberWithoutAbbreviation } from '../../../../../shared/formatNumber';
import { parseMaybeStringAsFloatOrDefault } from '../../../../../shared/numberUtil';

const ASSERTION_OPERATOR_TO_DESCRIPTION: Record<AssertionStdOperator, string | undefined> = {
    [AssertionStdOperator.EqualTo]: 'Is equal to',
    [AssertionStdOperator.NotEqualTo]: 'Is not equal to',
    [AssertionStdOperator.Contain]: 'Contains',
    [AssertionStdOperator.RegexMatch]: 'Matches',
    [AssertionStdOperator.StartWith]: 'Starts with',
    [AssertionStdOperator.EndWith]: 'Ends with',
    [AssertionStdOperator.In]: 'Is in',
    [AssertionStdOperator.NotIn]: 'Is not in',

    [AssertionStdOperator.IsFalse]: 'Is False',
    [AssertionStdOperator.IsTrue]: 'Is True',
    [AssertionStdOperator.Null]: 'Is NULL',
    [AssertionStdOperator.NotNull]: 'Is not NULL',

    [AssertionStdOperator.GreaterThan]: 'Is greater than',
    [AssertionStdOperator.GreaterThanOrEqualTo]: 'Is greater than or equal to',
    [AssertionStdOperator.LessThan]: 'Is less than',
    [AssertionStdOperator.LessThanOrEqualTo]: 'Is less than or equal to',
    [AssertionStdOperator.Between]: 'Is within a range',

    [AssertionStdOperator.Native]: undefined,
};

const SUPPORTED_OPERATORS_FOR_FIELD_DESCRIPTION = [
    AssertionStdOperator.EqualTo,
    AssertionStdOperator.Null,
    AssertionStdOperator.NotNull,
    AssertionStdOperator.NotEqualTo,
    AssertionStdOperator.NotIn,
    AssertionStdOperator.RegexMatch,
    AssertionStdOperator.GreaterThan,
    AssertionStdOperator.LessThan,
    AssertionStdOperator.GreaterThanOrEqualTo,
    AssertionStdOperator.LessThanOrEqualTo,
    AssertionStdOperator.In,
    AssertionStdOperator.Between,
    AssertionStdOperator.Contain,
    AssertionStdOperator.IsTrue,
    AssertionStdOperator.IsFalse,
];

const getAssertionStdOperator = (operator: AssertionStdOperator) => {
    if (!ASSERTION_OPERATOR_TO_DESCRIPTION[operator] || !SUPPORTED_OPERATORS_FOR_FIELD_DESCRIPTION.includes(operator)) {
        throw new Error(`Unknown operator ${operator}`);
    }
    return ASSERTION_OPERATOR_TO_DESCRIPTION[operator]?.toLowerCase();
};

export const getFieldMetricTypeReadableLabel = (metric: FieldMetricType) => {
    switch (metric) {
        case FieldMetricType.NullCount:
            return 'Null count';
        case FieldMetricType.NullPercentage:
            return 'Null percentage';
        case FieldMetricType.UniqueCount:
            return 'Unique count';
        case FieldMetricType.UniquePercentage:
            return 'Unique percentage';
        case FieldMetricType.MaxLength:
            return 'Max length';
        case FieldMetricType.MinLength:
            return 'Min length';
        case FieldMetricType.EmptyCount:
            return 'Empty count';
        case FieldMetricType.EmptyPercentage:
            return 'Empty percentage';
        case FieldMetricType.Max:
            return 'Max';
        case FieldMetricType.Min:
            return 'Min';
        case FieldMetricType.Mean:
            return 'Average';
        case FieldMetricType.Median:
            return 'Median';
        case FieldMetricType.NegativeCount:
            return 'Negative count';
        case FieldMetricType.NegativePercentage:
            return 'Negative percentage';
        case FieldMetricType.Stddev:
            return 'Standard deviation';
        case FieldMetricType.ZeroCount:
            return 'Zero count';
        case FieldMetricType.ZeroPercentage:
            return 'Zero percentage';
        default:
            throw new Error(`Unknown field metric type ${metric}`);
    }
};

const getFieldTransformType = (transform: FieldTransformType) => {
    switch (transform) {
        case FieldTransformType.Length:
            return 'Length';
        default:
            throw new Error(`Unknown field transform type ${transform}`);
    }
};

const getAssertionStdParameters = (parameters: AssertionStdParameters) => {
    if (parameters.value) {
        return formatNumberWithoutAbbreviation(
            parseMaybeStringAsFloatOrDefault(parameters.value.value, parameters.value.value),
        );
    }
    if (parameters.minValue && parameters.maxValue) {
        return `${formatNumberWithoutAbbreviation(
            parseMaybeStringAsFloatOrDefault(parameters.minValue.value, parameters.minValue.value),
        )} and ${formatNumberWithoutAbbreviation(
            parseMaybeStringAsFloatOrDefault(parameters.maxValue.value, parameters.maxValue.value),
        )}`;
    }
    return '';
};

export const getFieldDescription = (assertionInfo: FieldAssertionInfo) => {
    const { type, fieldValuesAssertion, fieldMetricAssertion } = assertionInfo;
    switch (type) {
        case FieldAssertionType.FieldValues:
            return fieldValuesAssertion?.field?.path;
        case FieldAssertionType.FieldMetric:
            return fieldMetricAssertion?.field?.path;
        default:
            throw new Error(`Unknown field assertion type ${type}`);
    }
};

export const getFieldOperatorDescription = (assertionInfo: FieldAssertionInfo) => {
    const { type, fieldValuesAssertion, fieldMetricAssertion } = assertionInfo;
    switch (type) {
        case FieldAssertionType.FieldValues:
            if (!fieldValuesAssertion?.operator) return '';
            return getAssertionStdOperator(fieldValuesAssertion.operator);
        case FieldAssertionType.FieldMetric:
            if (!fieldMetricAssertion?.operator) return '';
            return getAssertionStdOperator(fieldMetricAssertion.operator);
        default:
            throw new Error(`Unknown field assertion type ${type}`);
    }
};

export const getFieldTransformDescription = (assertionInfo: FieldAssertionInfo) => {
    const { type, fieldValuesAssertion, fieldMetricAssertion } = assertionInfo;
    switch (type) {
        case FieldAssertionType.FieldValues:
            if (!fieldValuesAssertion?.transform?.type) return '';
            return getFieldTransformType(fieldValuesAssertion.transform.type);
        case FieldAssertionType.FieldMetric:
            if (!fieldMetricAssertion?.metric) return '';
            return getFieldMetricTypeReadableLabel(fieldMetricAssertion.metric);
        default:
            throw new Error(`Unknown field assertion type ${type}`);
    }
};

export const getFieldParametersDescription = (assertionInfo: FieldAssertionInfo) => {
    const { type, fieldValuesAssertion, fieldMetricAssertion } = assertionInfo;
    switch (type) {
        case FieldAssertionType.FieldValues:
            if (!fieldValuesAssertion?.parameters) return '';
            return getAssertionStdParameters(fieldValuesAssertion.parameters);
        case FieldAssertionType.FieldMetric:
            if (!fieldMetricAssertion?.parameters) return '';
            return getAssertionStdParameters(fieldMetricAssertion.parameters);
        default:
            throw new Error(`Unknown field assertion type ${type}`);
    }
};
