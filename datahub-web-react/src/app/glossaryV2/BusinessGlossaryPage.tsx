import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';
import { useGetRootGlossaryNodesQuery, useGetRootGlossaryTermsQuery } from '../../graphql/glossary.generated';
import CreateGlossaryEntityModal from '../entityV2/shared/EntityDropdown/CreateGlossaryEntityModal';
import { EntityType } from '../../types.generated';
import { Message } from '../shared/Message';
import { sortGlossaryTerms } from '../entityV2/glossaryTerm/utils';
import { useEntityRegistry } from '../useEntityRegistry';
import { sortGlossaryNodes } from '../entityV2/glossaryNode/utils';
import {
    BUSINESS_GLOSSARY_INTRO_ID,
    BUSINESS_GLOSSARY_CREATE_TERM_ID,
    BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID,
} from '../onboarding/config/BusinessGlossaryOnboardingConfig';
import { OnboardingTour } from '../onboarding/OnboardingTour';
import { useGlossaryEntityData } from '../entityV2/shared/GlossaryEntityContext';
import { useUserContext } from '../context/useUserContext';
import GlossaryContentProvider from './GlossaryContentProvider';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';

const GlossaryWrapper = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    display: flex;
    flex: 1;
    height: 100%;
    background-color: white;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    ${(props) => props.$isShowNavBarRedesign && `box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']}`}
`;

const MainWrapper = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    flex: 1;
    margin: ${(props) => (props.$isShowNavBarRedesign ? '0' : '0 16px 12px 12px')};
`;

const BusinessGlossaryPage = () => {
    const {
        data: termsData,
        refetch: refetchForTerms,
        loading: termsLoading,
        error: termsError,
    } = useGetRootGlossaryTermsQuery();
    const {
        data: nodesData,
        refetch: refetchForNodes,
        loading: nodesLoading,
        error: nodesError,
    } = useGetRootGlossaryNodesQuery();
    const entityRegistry = useEntityRegistry();
    const { setEntityData } = useGlossaryEntityData();

    useEffect(() => {
        setEntityData(null);
    }, [setEntityData]);

    const terms = termsData?.getRootGlossaryTerms?.terms?.sort((termA, termB) =>
        sortGlossaryTerms(entityRegistry, termA, termB),
    );
    const nodes = nodesData?.getRootGlossaryNodes?.nodes?.sort((nodeA, nodeB) =>
        sortGlossaryNodes(entityRegistry, nodeA, nodeB),
    );

    const hasTermsOrNodes = !!nodes?.length || !!terms?.length;

    const [isCreateTermModalVisible, setIsCreateTermModalVisible] = useState(false);
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);

    const user = useUserContext();
    const canManageGlossaries = user?.platformPrivileges?.manageGlossaries;
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <>
            <OnboardingTour
                stepIds={[
                    BUSINESS_GLOSSARY_INTRO_ID,
                    BUSINESS_GLOSSARY_CREATE_TERM_ID,
                    BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID,
                ]}
            />
            <MainWrapper $isShowNavBarRedesign={isShowNavBarRedesign}>
                {/* TODO: Once the api for getting the stats data is available, we need to change this condition accordingly */}
                {/* {termsData?.getRootGlossaryTerms?.total !== 0 && (
                    <GlossaryStatsProvider
                        totalGlossaryTerms={200}
                        activeGlossaryTerms={90}
                        owners={10}
                        approvedGlossaryTerms={2}
                    />
                )} */}
                <GlossaryWrapper $isShowNavBarRedesign={isShowNavBarRedesign}>
                    {(termsLoading || nodesLoading) && (
                        <Message type="loading" content="Loading Glossary..." style={{ marginTop: '10%' }} />
                    )}
                    {(termsError || nodesError) && (
                        <Message type="error" content="Failed to load glossary! An unexpected error occurred." />
                    )}
                    <GlossaryContentProvider
                        setIsCreateNodeModalVisible={setIsCreateNodeModalVisible}
                        hasTermsOrNodes={hasTermsOrNodes}
                        nodes={nodes || []}
                        terms={terms || []}
                        termsLoading={termsLoading}
                        nodesLoading={nodesLoading}
                        refetchForNodes={refetchForNodes}
                        refetchForTerms={refetchForTerms}
                    />
                </GlossaryWrapper>
            </MainWrapper>
            {isCreateTermModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryTerm}
                    canCreateGlossaryEntity={!!canManageGlossaries}
                    onClose={() => setIsCreateTermModalVisible(false)}
                    refetchData={refetchForTerms}
                />
            )}
            {isCreateNodeModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryNode}
                    canCreateGlossaryEntity={!!canManageGlossaries}
                    onClose={() => setIsCreateNodeModalVisible(false)}
                    refetchData={refetchForNodes}
                    canSelectParentUrn={false}
                />
            )}
        </>
    );
};

export default BusinessGlossaryPage;
