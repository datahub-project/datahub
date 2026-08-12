import React from 'react';
import styled from 'styled-components';

import { CompactOutputPortsSection } from '@app/entityV2/dataProduct/CompactOutputPortsSection';
import { DataProductMetaRow } from '@app/entityV2/dataProduct/DataProductMetaRow';
import { LineagePanel } from '@app/entityV2/dataProduct/LineagePanel';
import { SubProductsSection } from '@app/entityV2/dataProduct/SubProductsSection';
import { SummaryTabWrapper } from '@app/entityV2/shared/summary/HeaderComponents';
import SummaryAboutSection from '@app/entityV2/shared/summary/SummaryAboutSection';

const ModulesRow = styled.div`
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    width: 100%;
`;

export const DataProductSummaryTab = () => {
    return (
        <SummaryTabWrapper>
            <DataProductMetaRow />
            <SummaryAboutSection />
            <ModulesRow>
                <CompactOutputPortsSection />
                <SubProductsSection />
            </ModulesRow>
            <LineagePanel />
        </SummaryTabWrapper>
    );
};
