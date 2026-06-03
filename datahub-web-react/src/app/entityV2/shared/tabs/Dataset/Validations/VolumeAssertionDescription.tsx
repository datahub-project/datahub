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

    /* eslint-disable i18next/no-literal-string -- (untranslated-text) Sentence assembled from type/operator/parameter/unit fragments in
       English word order; cannot be split for translation */
    return (
        <div>
            <Typography.Text>
                Table {volumeTypeDescription} {operatorDescription} {parameterDescription} {valueChangeTypeDescription}
            </Typography.Text>
        </div>
    );
    /* eslint-enable i18next/no-literal-string */
};
