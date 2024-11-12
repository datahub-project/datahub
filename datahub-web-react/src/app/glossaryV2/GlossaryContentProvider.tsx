import React from 'react';
import styled from 'styled-components/macro';
import { Button } from 'antd';
import { PageTitle } from '@src/alchemy-components/components/PageTitle';
import { GlossaryNodeFragment } from '../../graphql/fragments.generated';
import { ChildGlossaryTermFragment } from '../../graphql/glossaryNode.generated';
import AddGlossaryIcon from '../../images/add-term-group.svg?react';
import { GlossaryNode, GlossaryTerm } from '../../types.generated';
import { ANTD_GRAY, REDESIGN_COLORS } from '../entityV2/shared/constants';
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

const PrimaryButton = styled(Button)`
    color: ${ANTD_GRAY[1]};
    box-shadow: none;
    border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    transition: 0.15s;
    display: flex;
    align-items: center;

    &:hover {
        transition: 0.15s;
        opacity: 0.9;
        background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
        border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    }

    svg > g > path {
        fill: ${REDESIGN_COLORS.WHITE};
    }
`;

const ButtonContent = styled.div`
    display: flex;
    gap: 5px;
    align-items: center;
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
                    <PrimaryButton
                        data-testid="add-term-group-button-v2"
                        id={BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID}
                        type="primary"
                        size="large"
                        // can not be disabled on acryl-main due to ability to propose
                        onClick={() => setIsCreateNodeModalVisible(true)}
                    >
                        <ButtonContent>
                            <AddGlossaryIcon />
                            Glossary
                        </ButtonContent>
                    </PrimaryButton>
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
