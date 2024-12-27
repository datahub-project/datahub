import { Pill } from '@src/alchemy-components';
import { capitalizeFirstLetter } from '@src/app/shared/textUtil';
import { OperationType } from '@src/types.generated';
import React, { useMemo } from 'react';
import { AVAILABLE_OPERATION_TYPES } from '../../../constants';

type ChangeTypePillProps = {
    changeType: OperationType;
};

export default function ChangeTypePill({ changeType }: ChangeTypePillProps) {
    const changeTypeName = useMemo(() => capitalizeFirstLetter(changeType) as string, [changeType]);
    const colorSheme = useMemo(() => (changeType === OperationType.Delete ? 'red' : 'violet'), [changeType]);

    if (!AVAILABLE_OPERATION_TYPES.includes(changeType)) return null;

    return <Pill size="sm" label={changeTypeName} colorScheme={colorSheme} clickable={false} />;
}
