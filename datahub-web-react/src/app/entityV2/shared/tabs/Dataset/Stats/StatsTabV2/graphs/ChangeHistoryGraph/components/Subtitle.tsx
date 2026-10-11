import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import ChangeTypeSummaryPill from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/components/ChangeTypeSummaryPill';
import {
    AnyOperationType,
    OperationsData,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { OperationType } from '@src/types.generated';

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
    const { t } = useTranslation('entity.profile.stats');
    const operations = useMemo(() => {
        return Object.entries(summary?.operations || {})
            .map(([_, value]) => value)
            .filter((entry) => entry.type !== OperationType.Unknown)
            .filter((entry) => entry.value > 0)
            .sort((a, b) => b.value - a.value);
    }, [summary]);

    return (
        <Container>
            <span>{t('changeHistoryGraph.operationsMade')}</span>
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
