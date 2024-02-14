import React from 'react';
import { Button, Typography } from 'antd';
import styled from 'styled-components/macro';
import { PlusOutlined } from '@ant-design/icons';
import TabToolbar from '../entityV2/shared/components/styled/TabToolbar';
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
`;

const TitleContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const Title = styled(Typography.Title)`
    margin-bottom: 0 !important;
`;

const Subtitle = styled(Typography.Text)`
    font-size: 10px;
    font-weight: 400;
    line-height: 13px;
    color: #434863;
`;

const ButtonContainer = styled.div`
    display: flex;
    gap: 9px;
`;

const TransparentButton = styled(Button)`
    color: #533fd1;
    font-size: 12px;
    box-shadow: none;
    border-color: #533fd1;
    transition: 0.15s;

    &:hover {
        transition: 0.15s;
        opacity: 0.9;
        border-color: #533fd1;
        color: #533fd1;
    }
`;

const PrimaryButton = styled(Button)`
    color: #ffffff;
    font-size: 12px;
    box-shadow: none;
    border-color: #533fd1;
    background-color: #533fd1;
    transition: 0.15s;

    &:hover {
        transition: 0.15s;
        opacity: 0.9;
        background-color: #533fd1;
    }
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
                    <Title level={5}>Business Glossary</Title>
                    <Subtitle>View and modify your data dictionaries</Subtitle>
                </TitleContainer>
                <ButtonContainer>
                    <TransparentButton
                        data-testid="add-term-group-button"
                        id={BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID}
                        size="large"
                        // can not be disabled on acryl-main due to ability to propose
                        onClick={() => setIsCreateNodeModalVisible(true)}
                    >
                        <PlusOutlined style={{ fontSize: '12px' }} /> Add Term Group
                    </TransparentButton>
                    <PrimaryButton
                        data-testid="add-term-button"
                        id={BUSINESS_GLOSSARY_CREATE_TERM_ID}
                        type="primary"
                        size="large"
                        // can not be disabled on acryl-main due to ability to propose
                        onClick={() => setIsCreateTermModalVisible(true)}
                    >
                        <PlusOutlined style={{ fontSize: '12px' }} /> Add Term
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
