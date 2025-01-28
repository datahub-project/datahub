import React from 'react';
import styled from 'styled-components/macro';
import { PageTitle } from '@src/alchemy-components/components/PageTitle';
import { Button } from '@components';
import { GlossaryNodeFragment } from '../../graphql/fragments.generated';
import { ChildGlossaryTermFragment } from '../../graphql/glossaryNode.generated';
import { GlossaryNode, GlossaryTerm } from '../../types.generated';
import { BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID } from '../onboarding/config/BusinessGlossaryOnboardingConfig';
import EmptyGlossarySection from './EmptyGlossarySection';
import GlossaryEntitiesList from './GlossaryEntitiesList';

const MainContentWrapper = styled.div`
    display: flex;
    flex: 1;
    flex-direction: column;
`;

export const HeaderWrapper = styled.div`
    padding: 16px 20px 12px 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const ButtonContainer = styled.div`
    display: flex;
    gap: 9px;
`;

const ListWrapper = styled.div`
    padding: 4px 12px 12px 12px;
    overflow: auto;
`;

interface Props {
    setIsCreateNodeModalVisible: React.Dispatch<React.SetStateAction<boolean>>;
    hasTermsOrNodes: boolean;
    nodes: (GlossaryNode | GlossaryNodeFragment)[];
    terms: (GlossaryTerm | ChildGlossaryTermFragment)[];
    termsLoading: boolean;
    nodesLoading: boolean;
    refetchForTerms: () => void;
    refetchForNodes: () => void;
}

const GlossaryContentProvider = (props: Props) => {
    const {
        setIsCreateNodeModalVisible,
        hasTermsOrNodes,
        nodes,
        terms,
        termsLoading,
        nodesLoading,
        refetchForTerms,
        refetchForNodes,
    } = props;

    return (
        <MainContentWrapper data-testid="glossary-entities-list">
            <HeaderWrapper data-testid="glossaryPageV2">
                <PageTitle
                    title="Business Glossary"
                    subTitle="Classify your data assets and columns using data dictionaries"
                />
                <ButtonContainer>
                    <Button
                        data-testid="add-term-group-button-v2"
                        id={BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID}
                        size="lg"
                        icon="Add"
                        // can not be disabled on acryl-main due to ability to propose
                        onClick={() => setIsCreateNodeModalVisible(true)}
                    >
                        Create Glossary
                    </Button>
                </ButtonContainer>
            </HeaderWrapper>
            <ListWrapper>
                {hasTermsOrNodes && <GlossaryEntitiesList nodes={nodes || []} terms={terms || []} />}
            </ListWrapper>
            {!(termsLoading || nodesLoading) && !hasTermsOrNodes && (
                <EmptyGlossarySection
                    title="Empty Glossary"
                    description="Create Terms and Term Groups to organize data assets using a shared vocabulary."
                    refetchForTerms={refetchForTerms}
                    refetchForNodes={refetchForNodes}
                />
            )}
        </MainContentWrapper>
    );
};

export default GlossaryContentProvider;
