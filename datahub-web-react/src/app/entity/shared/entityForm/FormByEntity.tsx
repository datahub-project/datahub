import React from 'react';

import styled from 'styled-components';

import { useEntityRegistry } from '../../../useEntityRegistry';
import { EntityContext, useEntityContext } from '../EntityContext';
import { useEntityFormContext } from './EntityFormContext';

import { ANTD_GRAY_V2 } from '../constants';
import ProfileSidebar from '../containers/profile/sidebar/ProfileSidebar';
import EntityInfo from '../containers/profile/sidebar/EntityInfo/EntityInfo';

import Form from './Form';
import ProgressBar from './ProgressBar';
import { OnboardingTour } from '../../../onboarding/OnboardingTour';
import {
    FORM_QUESTION_VIEW_BUTTON,
    WELCOME_TO_BULK_BY_ENTITY_ID,
} from '../../../onboarding/config/FormOnboardingConfig';

import { useIsThemeV2 } from '../../../useIsThemeV2';

const ContentWrapper = styled.div`
    background-color: ${ANTD_GRAY_V2[1]};
    max-height: 100%;
    display: flex;
    flex-direction: column;
    width: 100%;
    flex: 1;
`;

const FlexWrapper = styled.div`
    display: flex;
    max-height: 100%;
    overflow: auto;
    width: 100%;
    flex: 1;
`;

interface Props {
    formUrn: string;
}

export default function FormByEntity({ formUrn }: Props) {
    const {
        entity: { selectedEntity, entityData: selectedEntityData },
    } = useEntityFormContext();
    const { entityType, loading, refetch } = useEntityContext();
    const entityRegistry = useEntityRegistry();
    const sidebarSections = entityRegistry.getSidebarSections(selectedEntity?.type || entityType);
    const isV2 = useIsThemeV2();

    // Used for v2 - removes repeated entity header (we use EntityInfo in this component)
    // SidebarEntityHeader is always the first index in sidebarSections, so remove it here
    // TODO (OBS-677): remove this logic once we get form info into V2 sidebar
    const cleanedSidebarSections = sidebarSections.slice(1);

    // Conditional sections based on theme version
    const sections = isV2 ? cleanedSidebarSections : sidebarSections;

    return (
        <EntityContext.Provider
            value={{
                urn: selectedEntity?.urn || '',
                entityType: selectedEntity?.type || entityType,
                entityData: selectedEntityData || null,
                loading,
                baseEntity: selectedEntityData,
                dataNotCombinedWithSiblings: selectedEntityData,
                routeToTab: () => {},
                refetch,
                lineage: undefined,
            }}
        >
            <OnboardingTour stepIds={[WELCOME_TO_BULK_BY_ENTITY_ID, FORM_QUESTION_VIEW_BUTTON]} />
            <ContentWrapper>
                <ProgressBar formUrn={formUrn} />
                <FlexWrapper>
                    <Form formUrn={formUrn} />
                    {selectedEntityData && (
                        <ProfileSidebar
                            sidebarSections={loading ? [] : sections}
                            topSection={{ component: () => <EntityInfo formUrn={formUrn} /> }}
                            backgroundColor="white"
                            loading={loading}
                        />
                    )}
                </FlexWrapper>
            </ContentWrapper>
        </EntityContext.Provider>
    );
}
