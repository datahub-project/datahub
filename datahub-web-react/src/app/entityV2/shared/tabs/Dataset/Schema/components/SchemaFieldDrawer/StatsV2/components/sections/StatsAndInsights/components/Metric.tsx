import React from 'react';
import { Text } from '@src/alchemy-components';
import styled from 'styled-components';

const MetricContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 16px;
    padding: 16px 0;
`;

interface MetricProps {
    label: string;
    value: string;
    dataTestId?: string;
}

export default function Metric({ label, value, dataTestId }: MetricProps) {
    return (
        <MetricContainer data-testid={dataTestId}>
            <Text weight="semiBold" color="gray" size="sm" data-testid="label">
                {label}
            </Text>
            <Text color="gray" size="sm" data-testid="value">
                {value}
            </Text>
        </MetricContainer>
    );
}
