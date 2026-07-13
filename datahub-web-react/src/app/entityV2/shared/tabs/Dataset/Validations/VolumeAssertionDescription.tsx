import { Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import {
    getIsRowCountChange,
    getParameterDescription,
    getParameterInterpolation,
    getVolumeTypeInfo,
} from '@app/entityV2/shared/tabs/Dataset/Validations/utils';

import {
    AssertionStdOperator,
    AssertionValueChangeType,
    IncrementingSegmentRowCountChange,
    RowCountChange,
    VolumeAssertionInfo,
} from '@types';

type Props = {
    assertionInfo: VolumeAssertionInfo;
};

// Maps the assertion operator to the operator portion of the description translation key. Returns a
// literal union (not `string`) so the composed `volumeDescription.*` key stays statically checkable
// once i18next typed resources are re-enabled.
const getOperatorKeyPart = (operator: AssertionStdOperator): 'AtLeast' | 'AtMost' | 'Between' => {
    switch (operator) {
        case AssertionStdOperator.GreaterThanOrEqualTo:
            return 'AtLeast';
        case AssertionStdOperator.LessThanOrEqualTo:
            return 'AtMost';
        case AssertionStdOperator.Between:
            return 'Between';
        default:
            throw new Error(`Unknown operator ${operator}`);
    }
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
    const operatorKeyPart = volumeTypeInfo ? getOperatorKeyPart(volumeTypeInfo.operator) : 'AtLeast';
    const interpolation = getParameterInterpolation(parameterDescription);

    let key: string;
    if (isChange) {
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
