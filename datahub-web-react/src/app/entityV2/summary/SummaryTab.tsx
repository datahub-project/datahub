import React from 'react';
import styled from 'styled-components';

import AboutSection from '@app/entityV2/summary/documentation/AboutSection';
import PropertiesHeader from '@app/entityV2/summary/properties/PropertiesHeader';
import { StyledDivider } from '@app/entityV2/summary/styledComponents';

const SummaryWrapper = styled.div`
    padding: 16px 20px;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

export default function SummaryTab() {
    return (
        <SummaryWrapper>
            <PropertiesHeader />
            <AboutSection />
            <StyledDivider />
        </SummaryWrapper>
    );
}
