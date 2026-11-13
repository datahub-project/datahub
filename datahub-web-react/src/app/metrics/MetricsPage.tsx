import React from 'react';
import styled from 'styled-components';

import MetricsMainContent from '@app/metrics/MetricsMainContent';

const ContentWrapper = styled.div`
    display: flex;
    flex: 1;
    height: 100%;
    min-height: 0;
    overflow: hidden;
`;

export default function MetricsPage() {
    return (
        <ContentWrapper data-testid="metrics-page">
            <MetricsMainContent />
        </ContentWrapper>
    );
}
