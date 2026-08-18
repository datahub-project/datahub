import { Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import {
    getIsRowCountChange,
    getParameterDescription,
    getParameterInterpolation,
    getVolumeOperatorKeyPart,
    getVolumeTypeInfo,
} from '@app/entityV2/shared/tabs/Dataset/Validations/utils';

import {
    AssertionValueChangeType,
    IncrementingSegmentRowCountChange,
    RowCountChange,
    VolumeAssertionInfo,
} from '@types';

type Props = {
    assertionInfo: VolumeAssertionInfo;
};

/**
 * A human-readable description of a Volume Assertion.
 */
export const VolumeAssertionDescription = ({ assertionInfo }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const volumeType = assertionInfo.type;
    const volumeTypeInfo = getVolumeTypeInfo(assertionInfo);
    const isChange = getIsRowCountChange(volumeType);
    const parameterDescription = volumeTypeInfo ? getParameterDescription(volumeTypeInfo.parameters) : undefined;
    const operatorKeyPart = volumeTypeInfo ? getVolumeOperatorKeyPart(volumeTypeInfo.operator) : null;
    const interpolation = getParameterInterpolation(parameterDescription);

    let key: string;
    if (!operatorKeyPart) {
        // Missing volume info or an operator outside the supported set — render a generic
        // description rather than composing a key that has no translation. A present volumeTypeInfo
        // with an unrecognized operator is genuinely unexpected (e.g. from a direct API write), so
        // surface it for debugging; a missing volumeTypeInfo is a normal empty state and stays quiet.
        if (volumeTypeInfo) {
            console.warn(`Unsupported volume assertion operator: ${volumeTypeInfo.operator}`);
        }
        key = 'volumeDescription.unknown';
    } else if (isChange) {
        const isPercentage =
            (volumeTypeInfo as RowCountChange | IncrementingSegmentRowCountChange).type ===
            AssertionValueChangeType.Percentage;
        key = `volumeDescription.change${operatorKeyPart}${isPercentage ? 'Percent' : 'Rows'}`;
    } else {
        key = `volumeDescription.total${operatorKeyPart}`;
    }

    return (
        <div>
            <Typography.Text>{t(key, interpolation)}</Typography.Text>
        </div>
    );
};
