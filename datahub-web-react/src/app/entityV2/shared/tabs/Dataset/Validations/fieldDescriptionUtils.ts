import i18next from 'i18next';

import { GET_ASSERTION_OPERATOR_TO_DESCRIPTION_MAP } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/constants';
import { formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';
import { parseMaybeStringAsFloatOrDefault } from '@app/shared/numberUtil';

import {
    AssertionStdOperator,
    AssertionStdParameterType,
    AssertionStdParameters,
    FieldAssertionInfo,
    FieldAssertionType,
    FieldMetricType,
    FieldTransformType,
} from '@types';

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
const getAssertionStdOperator = ({ operator, isPlural }: { operator: AssertionStdOperator; isPlural?: boolean }) => {
    const ASSERTION_OPERATOR_TO_DESCRIPTION = GET_ASSERTION_OPERATOR_TO_DESCRIPTION_MAP({ isPlural });

    if (!ASSERTION_OPERATOR_TO_DESCRIPTION[operator] || !SUPPORTED_OPERATORS_FOR_FIELD_DESCRIPTION.includes(operator)) {
        throw new Error(`Unknown operator ${operator}`);
    }
    return ASSERTION_OPERATOR_TO_DESCRIPTION[operator]?.toLowerCase();
};

export const getFieldMetricTypeReadableLabel = (metric: FieldMetricType) => {
    switch (metric) {
        case FieldMetricType.NullCount:
            return i18next.t('entity.profile.validations:fieldMetricType.nullCount');
        case FieldMetricType.NullPercentage:
            return i18next.t('entity.profile.validations:fieldMetricType.nullPercentage');
        case FieldMetricType.UniqueCount:
            return i18next.t('entity.profile.validations:fieldMetricType.uniqueCount');
        case FieldMetricType.UniquePercentage:
            return i18next.t('entity.profile.validations:fieldMetricType.uniquePercentage');
        case FieldMetricType.MaxLength:
            return i18next.t('entity.profile.validations:fieldMetricType.maxLength');
        case FieldMetricType.MinLength:
            return i18next.t('entity.profile.validations:fieldMetricType.minLength');
        case FieldMetricType.EmptyCount:
            return i18next.t('entity.profile.validations:fieldMetricType.emptyCount');
        case FieldMetricType.EmptyPercentage:
            return i18next.t('entity.profile.validations:fieldMetricType.emptyPercentage');
        case FieldMetricType.Max:
            return i18next.t('entity.profile.validations:fieldMetricType.max');
        case FieldMetricType.Min:
            return i18next.t('entity.profile.validations:fieldMetricType.min');
        case FieldMetricType.Mean:
            return i18next.t('entity.profile.validations:fieldMetricType.average');
        case FieldMetricType.Median:
            return i18next.t('entity.profile.validations:fieldMetricType.median');
        case FieldMetricType.NegativeCount:
            return i18next.t('entity.profile.validations:fieldMetricType.negativeCount');
        case FieldMetricType.NegativePercentage:
            return i18next.t('entity.profile.validations:fieldMetricType.negativePercentage');
        case FieldMetricType.Stddev:
            return i18next.t('entity.profile.validations:fieldMetricType.standardDeviation');
        case FieldMetricType.ZeroCount:
            return i18next.t('entity.profile.validations:fieldMetricType.zeroCount');
        case FieldMetricType.ZeroPercentage:
            return i18next.t('entity.profile.validations:fieldMetricType.zeroPercentage');
        default:
            throw new Error(`Unknown field metric type ${metric}`);
    }
};

const getFieldTransformType = (transform: FieldTransformType) => {
    switch (transform) {
        case FieldTransformType.Length:
            return i18next.t('entity.profile.validations:fieldTransformType.length');
        default:
            throw new Error(`Unknown field transform type ${transform}`);
    }
};

/* untranslated-text -- sentence fragment, ' and ' between range values cannot be independently translated */
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

export const getFieldOperatorDescription = ({
    assertionInfo,
    isPlural,
}: {
    assertionInfo: FieldAssertionInfo;
    isPlural?: boolean;
}) => {
    const { type, fieldValuesAssertion, fieldMetricAssertion } = assertionInfo;
    switch (type) {
        case FieldAssertionType.FieldValues:
            if (!fieldValuesAssertion?.operator) return '';
            return getAssertionStdOperator({ operator: fieldValuesAssertion.operator, isPlural });
        case FieldAssertionType.FieldMetric:
            if (!fieldMetricAssertion?.operator) return '';
            return getAssertionStdOperator({ operator: fieldMetricAssertion.operator, isPlural });
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

export type FieldOperatorKey =
    | 'between'
    | 'equalTo'
    | 'notEqualTo'
    | 'contains'
    | 'regexMatch'
    | 'in'
    | 'notIn'
    | 'null'
    | 'notNull'
    | 'isTrue'
    | 'isFalse'
    | 'greaterThan'
    | 'greaterThanOrEqualTo'
    | 'lessThan'
    | 'lessThanOrEqualTo';

export type FieldSubjectShape =
    | 'values'
    | 'valuesTransform'
    | 'metric'
    | 'valuesColumn'
    | 'valuesTransformColumn'
    | 'metricColumn';

export type FieldDescriptionDescriptor = {
    shape: FieldSubjectShape;
    operatorKey: FieldOperatorKey;
    field: string;
    transformLabelKey?: string;
    metricLabelKey?: string;
    tokens: { value: string; minValue: string; maxValue: string };
};

// Maps the 15 field-supported operators to their translation-key part. Operators absent here
// (startsWith/endsWith/native) are unsupported for field descriptions and make callers throw,
// which the component turns into the generic fallback description.
const FIELD_OPERATOR_KEY_BY_STD_OPERATOR: Partial<Record<AssertionStdOperator, FieldOperatorKey>> = {
    [AssertionStdOperator.Between]: 'between',
    [AssertionStdOperator.EqualTo]: 'equalTo',
    [AssertionStdOperator.NotEqualTo]: 'notEqualTo',
    [AssertionStdOperator.Contain]: 'contains',
    [AssertionStdOperator.RegexMatch]: 'regexMatch',
    [AssertionStdOperator.In]: 'in',
    [AssertionStdOperator.NotIn]: 'notIn',
    [AssertionStdOperator.Null]: 'null',
    [AssertionStdOperator.NotNull]: 'notNull',
    [AssertionStdOperator.IsTrue]: 'isTrue',
    [AssertionStdOperator.IsFalse]: 'isFalse',
    [AssertionStdOperator.GreaterThan]: 'greaterThan',
    [AssertionStdOperator.GreaterThanOrEqualTo]: 'greaterThanOrEqualTo',
    [AssertionStdOperator.LessThan]: 'lessThan',
    [AssertionStdOperator.LessThanOrEqualTo]: 'lessThanOrEqualTo',
};

const getFieldOperatorKey = (operator: AssertionStdOperator): FieldOperatorKey => {
    const key = FIELD_OPERATOR_KEY_BY_STD_OPERATOR[operator];
    if (!key) {
        throw new Error(`Unsupported field operator ${operator}`);
    }
    return key;
};

const FIELD_METRIC_LABEL_SUFFIX_BY_METRIC: Record<FieldMetricType, string> = {
    [FieldMetricType.NullCount]: 'nullCount',
    [FieldMetricType.NullPercentage]: 'nullPercentage',
    [FieldMetricType.UniqueCount]: 'uniqueCount',
    [FieldMetricType.UniquePercentage]: 'uniquePercentage',
    [FieldMetricType.MaxLength]: 'maxLength',
    [FieldMetricType.MinLength]: 'minLength',
    [FieldMetricType.EmptyCount]: 'emptyCount',
    [FieldMetricType.EmptyPercentage]: 'emptyPercentage',
    [FieldMetricType.Max]: 'max',
    [FieldMetricType.Min]: 'min',
    [FieldMetricType.Mean]: 'mean',
    [FieldMetricType.Median]: 'median',
    [FieldMetricType.NegativeCount]: 'negativeCount',
    [FieldMetricType.NegativePercentage]: 'negativePercentage',
    [FieldMetricType.Stddev]: 'stddev',
    [FieldMetricType.ZeroCount]: 'zeroCount',
    [FieldMetricType.ZeroPercentage]: 'zeroPercentage',
};

const getFieldMetricLabelKey = (metric: FieldMetricType): string => {
    const suffix = FIELD_METRIC_LABEL_SUFFIX_BY_METRIC[metric];
    if (!suffix) {
        throw new Error(`Unknown field metric type ${metric}`);
    }
    return `fieldDescription.metricLabel.${suffix}`;
};

const getFieldTransformLabelKey = (transform: FieldTransformType): string => {
    switch (transform) {
        case FieldTransformType.Length:
            return 'fieldDescription.transformLabel.length';
        default:
            throw new Error(`Unknown field transform type ${transform}`);
    }
};

const formatParameter = (parameter: { value: string; type: AssertionStdParameterType } | undefined | null): string => {
    if (!parameter) return '';
    return formatNumberWithoutAbbreviation(parseMaybeStringAsFloatOrDefault(parameter.value, parameter.value));
};

export const getFieldParameterTokens = (
    parameters: AssertionStdParameters | undefined | null,
): { value: string; minValue: string; maxValue: string } => ({
    value: formatParameter(parameters?.value),
    minValue: formatParameter(parameters?.minValue),
    maxValue: formatParameter(parameters?.maxValue),
});

/**
 * Resolves everything the FieldAssertionDescription component needs to render one full-sentence
 * translation key: the subject shape, the operator key, and the interpolation tokens. Throws on any
 * unsupported/unknown enum value so the component can fall back to a generic description.
 */
export const getFieldDescriptionDescriptor = (
    assertionInfo: FieldAssertionInfo,
    options?: { showColumnTag?: boolean },
): FieldDescriptionDescriptor => {
    const showColumnTag = options?.showColumnTag ?? false;
    const { type, fieldValuesAssertion, fieldMetricAssertion } = assertionInfo;
    const field = getFieldDescription(assertionInfo) ?? '';

    switch (type) {
        case FieldAssertionType.FieldValues: {
            const operator = fieldValuesAssertion?.operator;
            if (!operator) throw new Error('Missing field operator');
            const operatorKey = getFieldOperatorKey(operator);
            const tokens = getFieldParameterTokens(fieldValuesAssertion?.parameters);
            const transformType = fieldValuesAssertion?.transform?.type;
            if (transformType) {
                return {
                    shape: showColumnTag ? 'valuesTransformColumn' : 'valuesTransform',
                    operatorKey,
                    field,
                    transformLabelKey: getFieldTransformLabelKey(transformType),
                    tokens,
                };
            }
            return {
                shape: showColumnTag ? 'valuesColumn' : 'values',
                operatorKey,
                field,
                tokens,
            };
        }
        case FieldAssertionType.FieldMetric: {
            const operator = fieldMetricAssertion?.operator;
            if (!operator) throw new Error('Missing field operator');
            const operatorKey = getFieldOperatorKey(operator);
            const metric = fieldMetricAssertion?.metric;
            if (!metric) throw new Error('Missing field metric');
            return {
                shape: showColumnTag ? 'metricColumn' : 'metric',
                operatorKey,
                field,
                metricLabelKey: getFieldMetricLabelKey(metric),
                tokens: getFieldParameterTokens(fieldMetricAssertion?.parameters),
            };
        }
        default:
            throw new Error(`Unknown field assertion type ${type}`);
    }
};
