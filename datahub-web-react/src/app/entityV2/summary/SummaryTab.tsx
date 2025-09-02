import React from 'react';
import styled from 'styled-components';

import AboutSection from '@app/entityV2/summary/documentation/AboutSection';
import Links from '@app/entityV2/summary/links/Links';
import PropertiesHeader from '@app/entityV2/summary/properties/PropertiesHeader';
import { StyledDivider } from '@app/entityV2/summary/styledComponents';
import { PageTemplateProvider } from '@app/homeV3/context/PageTemplateContext';
import { PageTemplateSurfaceType } from '@types';
import Template from '@app/homeV3/template/Template';

const SummaryWrapper = styled.div`
    padding: 16px 20px;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

interface Props {
    hideLinksButton?: boolean;
}

export default function SummaryTab({ properties }: { properties?: Props }) {
    const hideLinksButton = properties?.hideLinksButton;

    return (
        <PageTemplateProvider templateType={PageTemplateSurfaceType.AssetSummary}>
            <SummaryWrapper>
                <PropertiesHeader />
                <AboutSection />
                {!hideLinksButton && <Links />}
                <StyledDivider />
                <Template />
            </SummaryWrapper>
        </PageTemplateProvider>
    );
}
