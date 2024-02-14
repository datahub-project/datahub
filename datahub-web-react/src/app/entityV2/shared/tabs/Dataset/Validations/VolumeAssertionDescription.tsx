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
import { getIsRowCountChange, getVolumeTypeInfo } from './assertion/builder/steps/volume/utils';

type Props = {
    assertionInfo: VolumeAssertionInfo;
};

const getVolumeTypeDescription = (volumeType: VolumeAssertionType) => {
    switch (volumeType) {
        case VolumeAssertionType.RowCountTotal:
        case VolumeAssertionType.IncrementingSegmentRowCountTotal:
            return 'has';
        case VolumeAssertionType.RowCountChange:
        case VolumeAssertionType.IncrementingSegmentRowCountChange:
            return 'grows by';
        default:
            throw new Error(`Unknown volume type ${volumeType}`);
    }
};

const getOperatorDescription = (operator: AssertionStdOperator) => {
    switch (operator) {
        case AssertionStdOperator.GreaterThanOrEqualTo:
            return 'greater than or equal to';
        case AssertionStdOperator.LessThanOrEqualTo:
            return 'less than or equal to';
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
        return parameters.value.value;
    }
    if (parameters.minValue && parameters.maxValue) {
        return `${parameters.minValue.value} and ${parameters.maxValue.value}`;
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
    const operatorDescription = getOperatorDescription(volumeTypeInfo.operator);
    const parameterDescription = getParameterDescription(volumeTypeInfo.parameters);
    const valueChangeTypeDescription = getIsRowCountChange(volumeType)
        ? getValueChangeTypeDescription((volumeTypeInfo as RowCountChange | IncrementingSegmentRowCountChange).type)
        : 'rows';

    return (
        <div>
            <Typography.Text>
                Dataset {volumeTypeDescription} {operatorDescription} {parameterDescription}{' '}
                {valueChangeTypeDescription}
            </Typography.Text>
        </div>
    );
};
