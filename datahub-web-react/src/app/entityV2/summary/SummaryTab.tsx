import { colors } from '@components';
import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import AboutSection from '@app/entityV2/summary/documentation/AboutSection';
import Links from '@app/entityV2/summary/links/Links';
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

interface Props {
    hideLinksButton?: boolean;
}

export default function SummaryTab({ properties }: { properties?: Props }) {
    const hideLinksButton = properties?.hideLinksButton;

    return (
        <SummaryWrapper>
            <PropertiesHeader />
            <StyledDivider />
            <AboutSection />
            {!hideLinksButton && <Links />}
            <StyledDivider />
        </SummaryWrapper>
    );
}
