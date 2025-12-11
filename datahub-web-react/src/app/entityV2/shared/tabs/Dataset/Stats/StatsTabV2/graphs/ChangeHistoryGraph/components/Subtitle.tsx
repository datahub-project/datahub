/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useMemo } from 'react';
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
