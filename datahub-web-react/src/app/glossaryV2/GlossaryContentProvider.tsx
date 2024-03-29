import React from 'react';
import { Button, Typography } from 'antd';
import styled from 'styled-components/macro';
import TabToolbar from '../entityV2/shared/components/styled/TabToolbar';
import { REDESIGN_COLORS, ANTD_GRAY } from '../entityV2/shared/constants';
import TermGroupIcon from '../../images/glossary_collections_bookmark.svg?react';
import TermIcon from '../../images/collections_bookmark.svg?react';
import {
    BUSINESS_GLOSSARY_CREATE_TERM_ID,
    BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID,
} from '../onboarding/config/BusinessGlossaryOnboardingConfig';
import GlossaryEntitiesList from './GlossaryEntitiesList';
import EmptyGlossarySection from './EmptyGlossarySection';
import { GlossaryNode, GlossaryTerm } from '../../types.generated';
import { GlossaryNodeFragment } from '../../graphql/fragments.generated';
import { ChildGlossaryTermFragment } from '../../graphql/glossaryNode.generated';
import { GetRootGlossaryTermsQuery } from '../../graphql/glossary.generated';

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
    color: ${REDESIGN_COLORS.SECONDARY_PURPLE};
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

const TransparentButton = styled(Button)`
    color: ${REDESIGN_COLORS.COLD_GREY_TEXT};
    box-shadow: none;
    border-color: ${REDESIGN_COLORS.COLD_GREY_TEXT};
    transition: 0.15s;
    display: flex;
    align-items: center;

    &:hover {
        transition: 0.15s;
        opacity: 0.9;
        border-color: ${REDESIGN_COLORS.TITLE_PURPLE};
        color: ${REDESIGN_COLORS.TITLE_PURPLE};
        svg > g > path {
            fill: ${REDESIGN_COLORS.TITLE_PURPLE};
        }
    }

    svg > g > path {
        fill: ${REDESIGN_COLORS.COLD_GREY_TEXT};
    }
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
    setIsCreateTermModalVisible: React.Dispatch<React.SetStateAction<boolean>>;
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
        setIsCreateTermModalVisible,
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
                    <Title>Business Glossary</Title>
                    <Subtitle>View and modify your data dictionaries</Subtitle>
                </TitleContainer>
                <ButtonContainer>
                    <TransparentButton
                        data-testid="add-term-button"
                        id={BUSINESS_GLOSSARY_CREATE_TERM_ID}
                        size="large"
                        // can not be disabled on acryl-main due to ability to propose
                        onClick={() => setIsCreateTermModalVisible(true)}
                    >
                        <ButtonContent>
                            <TermIcon /> Term
                        </ButtonContent>
                    </TransparentButton>
                    <PrimaryButton
                        data-testid="add-term-group-button"
                        id={BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID}
                        type="primary"
                        size="large"
                        // can not be disabled on acryl-main due to ability to propose
                        onClick={() => setIsCreateNodeModalVisible(true)}
                    >
                        <ButtonContent>
                            <TermGroupIcon />
                            Term Group
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
