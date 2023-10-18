import {
    AssertionStdOperator,
    AssertionStdParameters,
    FieldAssertionInfo,
    FieldAssertionType,
    FieldMetricType,
    FieldTransformType,
} from '../../../../../../types.generated';

const getAssertionStdOperator = (operator: AssertionStdOperator) => {
    switch (operator) {
        case AssertionStdOperator.EqualTo:
            return 'should be equal to';
        case AssertionStdOperator.Null:
            return 'should be null';
        case AssertionStdOperator.NotNull:
            return 'should not be null';
        case AssertionStdOperator.NotEqualTo:
            return 'should not be equal to';
        case AssertionStdOperator.NotIn:
            return 'should not be in';
        case AssertionStdOperator.RegexMatch:
            return 'should match regex';
        case AssertionStdOperator.GreaterThan:
            return 'should be greater than';
        case AssertionStdOperator.LessThan:
            return 'should be less than';
        case AssertionStdOperator.GreaterThanOrEqualTo:
            return 'should be greater than or equal to';
        case AssertionStdOperator.LessThanOrEqualTo:
            return 'should be less than or equal to';
        case AssertionStdOperator.In:
            return 'should be in';
        case AssertionStdOperator.Between:
            return 'should be between';
        case AssertionStdOperator.Contain:
            return 'should contain';
        case AssertionStdOperator.IsTrue:
            return 'should be true';
        case AssertionStdOperator.IsFalse:
            return 'should be false';
        default:
            throw new Error(`Unknown operator ${operator}`);
    }
};

const getMetricType = (metric: FieldMetricType) => {
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
            return 'Mean';
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
        return parameters.value.value;
    }
    if (parameters.minValue && parameters.maxValue) {
        return `${parameters.minValue.value} and ${parameters.maxValue.value}`;
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
            return getMetricType(fieldMetricAssertion.metric);
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
