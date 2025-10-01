import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import AboutSection from '@app/entityV2/summary/documentation/AboutSection';
import PropertiesHeader from '@app/entityV2/summary/properties/PropertiesHeader';
import { StyledDivider } from '@app/entityV2/summary/styledComponents';
import { PageTemplateProvider } from '@app/homeV3/context/PageTemplateContext';
import Template from '@app/homeV3/template/Template';

import { PageTemplateSurfaceType } from '@types';

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
    const { urn } = useEntityData();

    return (
        <PageTemplateProvider templateType={PageTemplateSurfaceType.AssetSummary}>
            <SummaryWrapper>
                <PropertiesHeader />
                <AboutSection hideLinksButton={!!properties?.hideLinksButton} key={urn} />
                <StyledDivider />
                <Template />
            </SummaryWrapper>
        </PageTemplateProvider>
    );
}
