import { Button, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { GlossaryNodeFragment } from '../../graphql/fragments.generated';
import { GetRootGlossaryTermsQuery } from '../../graphql/glossary.generated';
import { ChildGlossaryTermFragment } from '../../graphql/glossaryNode.generated';
import AddGlossaryIcon from '../../images/add-term-group.svg?react';
import { GlossaryNode, GlossaryTerm } from '../../types.generated';
import TabToolbar from '../entityV2/shared/components/styled/TabToolbar';
import { ANTD_GRAY, REDESIGN_COLORS } from '../entityV2/shared/constants';
import { BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID } from '../onboarding/config/BusinessGlossaryOnboardingConfig';
import EmptyGlossarySection from './EmptyGlossarySection';
import GlossaryEntitiesList from './GlossaryEntitiesList';

const MainContentWrapper = styled.div`
    display: flex;
    flex: 1;
    flex-direction: column;
`;

export const HeaderWrapper = styled(TabToolbar)`
    padding: 17px 15px 19px 24px;
    height: auto;
    box-shadow: none;
    margin: 0 !important;
    border-bottom: 1px solid ${REDESIGN_COLORS.BORDER_2};
`;

const TitleContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const Title = styled(Typography.Text)`
    margin-bottom: 0 !important;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    font-size: 16px;
    font-weight: 700;
`;

const Subtitle = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 400;
    line-height: 13px;
    color: ${REDESIGN_COLORS.SUB_TEXT};
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

interface Props {
    setIsCreateNodeModalVisible: React.Dispatch<React.SetStateAction<boolean>>;
    hasTermsOrNodes: boolean;
    nodes: (GlossaryNode | GlossaryNodeFragment)[];
    terms: (GlossaryTerm | ChildGlossaryTermFragment)[];
    termsData: GetRootGlossaryTermsQuery | undefined;
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
        termsData,
        termsLoading,
        nodesLoading,
        refetchForTerms,
        refetchForNodes,
    } = props;

    return (
        <MainContentWrapper data-testid="glossary-entities-list">
            <HeaderWrapper>
                <TitleContainer>
                    <Title data-testid="glossaryPageV2">Business Glossary</Title>
                    <Subtitle>View and modify your glossaries</Subtitle>
                </TitleContainer>
                <ButtonContainer>
                    <PrimaryButton
                        data-testid="add-term-group-button"
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
            {hasTermsOrNodes && (
                <GlossaryEntitiesList
                    nodes={nodes || []}
                    terms={terms || []}
                    termsTotal={termsData?.getRootGlossaryTerms?.total}
                />
            )}
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
