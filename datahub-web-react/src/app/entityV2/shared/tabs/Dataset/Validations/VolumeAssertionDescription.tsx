/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';

import {
    getIsRowCountChange,
    getOperatorDescription,
    getParameterDescription,
    getValueChangeTypeDescription,
    getVolumeTypeDescription,
    getVolumeTypeInfo,
} from '@app/entityV2/shared/tabs/Dataset/Validations/utils';

import { IncrementingSegmentRowCountChange, RowCountChange, VolumeAssertionInfo } from '@types';

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
