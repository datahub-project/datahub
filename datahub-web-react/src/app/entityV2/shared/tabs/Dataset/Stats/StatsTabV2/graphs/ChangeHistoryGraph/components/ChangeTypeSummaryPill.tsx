import { Pill } from '@src/alchemy-components';
import { abbreviateNumber } from '@src/app/dataviz/utils';
import { pluralize } from '@src/app/shared/textUtil';
import { OperationType } from '@src/types.generated';
import React, { useMemo } from 'react';
import { getOperationName, getPillColorByOperationType } from './utils';

type ChangeTypeSummaryPillProps = {
    numberOfOperations: number;
    operationType: OperationType;
    onClick?: () => void;
};

export default function ChangeTypeSummaryPill({
    numberOfOperations,
    operationType,
    onClick,
}: ChangeTypeSummaryPillProps) {
    const label = useMemo(() => {
        const operationTypeName = getOperationName(operationType);
        return `${abbreviateNumber(numberOfOperations)} ${pluralize(numberOfOperations, operationTypeName)}`;
    }, [operationType, numberOfOperations]);

    const colorSheme = useMemo(() => getPillColorByOperationType(operationType), [operationType]);

    return <Pill size="sm" label={label} colorScheme={colorSheme} clickable={!!onClick} onPillClick={onClick} />;
}
