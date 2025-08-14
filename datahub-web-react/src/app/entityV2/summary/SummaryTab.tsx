import { colors } from '@components';
import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import AboutSection from '@app/entityV2/summary/documentation/AboutSection';
import PropertiesHeader from '@app/entityV2/summary/properties/PropertiesHeader';

const SummaryWrapper = styled.div`
    padding: 16px 20px;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const StyledDivider = styled(Divider)`
    margin: 0;
    color: ${colors.gray[100]};
`;

export default function SummaryTab() {
    return (
        <SummaryWrapper>
            <PropertiesHeader />
            <StyledDivider />
            <AboutSection />
            <StyledDivider />
        </SummaryWrapper>
    );
}
