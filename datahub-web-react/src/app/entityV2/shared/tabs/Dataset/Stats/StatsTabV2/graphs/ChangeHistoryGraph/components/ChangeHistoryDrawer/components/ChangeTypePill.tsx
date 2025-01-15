import { Pill } from '@src/alchemy-components';
import { capitalizeFirstLetter } from '@src/app/shared/textUtil';
import { OperationType } from '@src/types.generated';
import React, { useMemo } from 'react';
import { AnyOperationType } from '../../../types';

type ChangeTypePillProps = {
    changeType: AnyOperationType;
};

export default function ChangeTypePill({ changeType }: ChangeTypePillProps) {
    const changeTypeName = useMemo(() => capitalizeFirstLetter(changeType) as string, [changeType]);
    const colorSheme = useMemo(() => {
        switch (changeType) {
            case OperationType.Delete:
            case OperationType.Drop:
                return 'red';
            default:
                return 'violet';
        }
    }, [changeType]);

    return <Pill size="sm" label={changeTypeName} colorScheme={colorSheme} clickable={false} />;
}
