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
    /** Hide the About-section edit pencil (MVP read-only entities). */
    hideEditDescription?: boolean;
    /** Optional content rendered between PropertiesHeader and AboutSection. */
    preContent?: React.ReactNode;
}

export default function SummaryTab({ properties }: { properties?: Props }) {
    const { urn } = useEntityData();

    return (
        <PageTemplateProvider templateType={PageTemplateSurfaceType.AssetSummary}>
            <SummaryWrapper>
                <PropertiesHeader />
                {properties?.preContent}
                {properties?.preContent && <StyledDivider />}
                <AboutSection
                    hideLinksButton={!!properties?.hideLinksButton}
                    hideEditDescription={!!properties?.hideEditDescription}
                    key={urn}
                />
                <StyledDivider />
                <Template />
            </SummaryWrapper>
        </PageTemplateProvider>
    );
}
