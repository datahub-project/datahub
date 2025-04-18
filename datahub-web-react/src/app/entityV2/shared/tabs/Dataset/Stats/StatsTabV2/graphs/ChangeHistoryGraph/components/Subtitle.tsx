import { OperationType } from '@src/types.generated';
import React, { useMemo } from 'react';
import styled from 'styled-components';
import { AnyOperationType, OperationsData } from '../types';
import ChangeTypeSummaryPill from './ChangeTypeSummaryPill';

const Container = styled.div`
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
    gap: 4px;
`;

type SubtitleProps = {
    summary?: OperationsData;
    onTypeClick?: (operationType: string) => void;
    selectedOperationTypes: AnyOperationType[];
};

export default function Subtitle({ summary, onTypeClick, selectedOperationTypes }: SubtitleProps) {
    const operations = useMemo(() => {
        return Object.entries(summary?.operations || {})
            .map(([_, value]) => value)
            .filter((entry) => entry.type !== OperationType.Unknown)
            .filter((entry) => entry.value > 0)
            .sort((a, b) => b.value - a.value);
    }, [summary]);

    return (
        <Container>
            <span>Operations made to this table:</span>
            {operations.map((operation) => (
                <ChangeTypeSummaryPill
                    key={operation.key}
                    operation={operation}
                    onClick={() => onTypeClick?.(operation.key)}
                    selected={selectedOperationTypes.includes(operation.key)}
                />
            ))}
        </Container>
    );
}
