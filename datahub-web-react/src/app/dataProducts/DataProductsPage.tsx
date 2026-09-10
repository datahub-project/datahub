import React from 'react';
import styled from 'styled-components';

import DataProductsMainContent from '@app/dataProducts/DataProductsMainContent';

const ContentWrapper = styled.div`
    display: flex;
    flex: 1;
    height: 100%;
    min-height: 0;
    overflow: hidden;
`;

export default function DataProductsPage() {
    return (
        <ContentWrapper data-testid="data-products-page">
            <DataProductsMainContent />
        </ContentWrapper>
    );
}
