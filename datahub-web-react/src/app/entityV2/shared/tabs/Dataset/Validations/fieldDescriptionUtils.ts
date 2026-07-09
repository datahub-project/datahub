import i18next from 'i18next';

import { GET_ASSERTION_OPERATOR_TO_DESCRIPTION_MAP } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/constants';
import { formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';
import { parseMaybeStringAsFloatOrDefault } from '@app/shared/numberUtil';

import {
    AssertionStdOperator,
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
