import React from 'react';
import { Typography } from 'antd';
import {
    IncrementingSegmentRowCountChange,
    RowCountChange,
    VolumeAssertionInfo,
} from '../../../../../../types.generated';
import {
    getIsRowCountChange,
    getOperatorDescription,
    getParameterDescription,
    getValueChangeTypeDescription,
    getVolumeTypeDescription,
    getVolumeTypeInfo,
} from './utils';

type Props = {
    assertionInfo: VolumeAssertionInfo;
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
