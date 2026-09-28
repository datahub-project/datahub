import React from 'react';
import styled from 'styled-components';

import MarketplaceMainContent from '@app/marketplace/MarketplaceMainContent';

const ContentWrapper = styled.div`
    display: flex;
    flex: 1;
    height: 100%;
    min-height: 0;
    overflow: hidden;
`;

export default function MarketplacePage() {
    return (
        <ContentWrapper data-testid="marketplace-page">
            <MarketplaceMainContent />
        </ContentWrapper>
    );
}
