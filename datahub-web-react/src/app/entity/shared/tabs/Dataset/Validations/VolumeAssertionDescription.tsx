import React from 'react';
import { Typography } from 'antd';
import {
    AssertionStdOperator,
    AssertionStdParameters,
    AssertionValueChangeType,
    IncrementingSegmentRowCountChange,
    RowCountChange,
    VolumeAssertionInfo,
    VolumeAssertionType,
} from '../../../../../../types.generated';
import { formatNumberWithoutAbbreviation } from '../../../../../shared/formatNumber';
import { parseMaybeStringAsFloatOrDefault } from '../../../../../shared/numberUtil';

type Props = {
    assertionInfo: VolumeAssertionInfo;
};

type VolumeTypeField =
    | 'rowCountTotal'
    | 'rowCountChange'
    | 'incrementingSegmentRowCountTotal'
    | 'incrementingSegmentRowCountChange';

const getPropertyFromVolumeType = (type: VolumeAssertionType) => {
    switch (type) {
        case VolumeAssertionType.RowCountTotal:
            return 'rowCountTotal' as VolumeTypeField;
        case VolumeAssertionType.RowCountChange:
            return 'rowCountChange' as VolumeTypeField;
        case VolumeAssertionType.IncrementingSegmentRowCountTotal:
            return 'incrementingSegmentRowCountTotal' as VolumeTypeField;
        case VolumeAssertionType.IncrementingSegmentRowCountChange:
            return 'incrementingSegmentRowCountChange' as VolumeTypeField;
        default:
            throw new Error(`Unknown volume assertion type: ${type}`);
    }
};

const getVolumeTypeInfo = (volumeAssertion: VolumeAssertionInfo) => {
    const result = volumeAssertion[getPropertyFromVolumeType(volumeAssertion.type)];
    if (!result) {
        return undefined;
    }
    return result;
};

const getIsRowCountChange = (type: VolumeAssertionType) => {
    return [VolumeAssertionType.RowCountChange, VolumeAssertionType.IncrementingSegmentRowCountChange].includes(type);
};

const getVolumeTypeDescription = (volumeType: VolumeAssertionType) => {
    switch (volumeType) {
        case VolumeAssertionType.RowCountTotal:
        case VolumeAssertionType.IncrementingSegmentRowCountTotal:
            return 'has';
        case VolumeAssertionType.RowCountChange:
        case VolumeAssertionType.IncrementingSegmentRowCountChange:
            return 'should grow by';
        default:
            throw new Error(`Unknown volume type ${volumeType}`);
    }
};

const getOperatorDescription = (operator: AssertionStdOperator) => {
    switch (operator) {
        case AssertionStdOperator.GreaterThanOrEqualTo:
            return 'at least';
        case AssertionStdOperator.LessThanOrEqualTo:
            return 'at most';
        case AssertionStdOperator.Between:
            return 'between';
        default:
            throw new Error(`Unknown operator ${operator}`);
    }
};

const getValueChangeTypeDescription = (valueChangeType: AssertionValueChangeType) => {
    switch (valueChangeType) {
        case AssertionValueChangeType.Absolute:
            return 'rows';
        case AssertionValueChangeType.Percentage:
            return '%';
        default:
            throw new Error(`Unknown value change type ${valueChangeType}`);
    }
};

const getParameterDescription = (parameters: AssertionStdParameters) => {
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
    throw new Error('Invalid assertion parameters provided');
};

/**
 * A human-readable description of a Volume Assertion.
 */
export const VolumeAssertionDescription = ({ assertionInfo }: Props) => {
    const volumeType = assertionInfo.type;
    const volumeTypeInfo = getVolumeTypeInfo(assertionInfo);
    const volumeTypeDescription = getVolumeTypeDescription(volumeType);
    const operatorDescription = volumeTypeInfo ? getOperatorDescription(volumeTypeInfo.operator) : '';
    const parameterDescription = volumeTypeInfo ? getParameterDescription(volumeTypeInfo.parameters) : '';
    const valueChangeTypeDescription = getIsRowCountChange(volumeType)
        ? getValueChangeTypeDescription((volumeTypeInfo as RowCountChange | IncrementingSegmentRowCountChange).type)
        : 'rows';

    return (
        <div>
            <Typography.Text>
                Table {volumeTypeDescription} {operatorDescription} {parameterDescription} {valueChangeTypeDescription}
            </Typography.Text>
        </div>
    );
};
