/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { Text } from '@src/alchemy-components';

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
