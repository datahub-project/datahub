import React from 'react';
import { OperationType } from '@src/types.generated';
import styled from 'styled-components';
import { CalendarData } from '@src/alchemy-components/components/CalendarChart/types';
import useOperationsSummary from '../hooks/useOperationsSummary';
import ChangeTypeSummaryPill from './ChangeTypeSummaryPill';
import { ValueType } from '../hooks/useChangeHistoryData';

const Container = styled.div`
    display: flex;
    flex-direction: row;
    gap: 4px;
`;

type SubtitleProps = {
    data: CalendarData<ValueType>[];
    onTypeClick?: (operationType: OperationType) => void;
};

export default function Subtitle({ data, onTypeClick }: SubtitleProps) {
    const { inserts, updates, deletes } = useOperationsSummary(data);

    return (
        <Container>
            <span>Insert update and delete operations made to this table:</span>
            <ChangeTypeSummaryPill
                numberOfOperations={updates}
                operationType={OperationType.Update}
                onClick={() => onTypeClick?.(OperationType.Update)}
            />
            <ChangeTypeSummaryPill
                numberOfOperations={inserts}
                operationType={OperationType.Insert}
                onClick={() => onTypeClick?.(OperationType.Insert)}
            />
            <ChangeTypeSummaryPill
                numberOfOperations={deletes}
                operationType={OperationType.Delete}
                onClick={() => onTypeClick?.(OperationType.Delete)}
            />
        </Container>
    );
}
