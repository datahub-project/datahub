import { Button } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import EmptyGlossarySection from '@app/glossaryV2/EmptyGlossarySection';
import GlossaryEntitiesList from '@app/glossaryV2/GlossaryEntitiesList';
import { BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID } from '@app/onboarding/config/BusinessGlossaryOnboardingConfig';
import { PageTitle } from '@src/alchemy-components/components/PageTitle';

import { GlossaryNodeFragment } from '@graphql/fragments.generated';
import { ChildGlossaryTermFragment } from '@graphql/glossaryNode.generated';
import { GlossaryNode, GlossaryTerm } from '@types';

const MainContentWrapper = styled.div`
    display: flex;
    flex: 1;
    flex-direction: column;
`;

const HeaderWrapper = styled.div`
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
    padding: 4px 20px 12px 20px;
    overflow: auto;
`;

interface Props {
    setIsCreateNodeModalVisible: React.Dispatch<React.SetStateAction<boolean>>;
    setIsCreateTermModalVisible: React.Dispatch<React.SetStateAction<boolean>>;
    hasTermsOrNodes: boolean;
    nodes: (GlossaryNode | GlossaryNodeFragment)[];
    terms: (GlossaryTerm | ChildGlossaryTermFragment)[];
    termsLoading: boolean;
    nodesLoading: boolean;
}

const GlossaryContentProvider = (props: Props) => {
    const { t } = useTranslation('governance.glossary');
    const {
        setIsCreateNodeModalVisible,
        setIsCreateTermModalVisible,
        hasTermsOrNodes,
        nodes,
        terms,
        termsLoading,
        nodesLoading,
    } = props;

    return (
        <MainContentWrapper data-testid="glossary-entities-list">
            <HeaderWrapper data-testid="glossaryPageV2">
                <PageTitle title={t('page.title')} subTitle={t('page.subtitle')} />
                <ButtonContainer>
                    <Button
                        data-testid="add-term-group-button-v2"
                        id={BUSINESS_GLOSSARY_CREATE_TERM_GROUP_ID}
                        size="md"
                        icon={{ icon: Plus }}
                        // can not be disabled on acryl-main due to ability to propose
                        onClick={() => setIsCreateNodeModalVisible(true)}
                    >
                        {t('page.createGlossary')}
                    </Button>
                </ButtonContainer>
            </HeaderWrapper>
            <ListWrapper>
                {hasTermsOrNodes && <GlossaryEntitiesList nodes={nodes || []} terms={terms || []} />}
            </ListWrapper>
            {!(termsLoading || nodesLoading) && !hasTermsOrNodes && (
                <EmptyGlossarySection
                    title={t('empty.title')}
                    description={t('empty.description')}
                    onAddTerm={() => setIsCreateTermModalVisible(true)}
                    onAddtermGroup={() => setIsCreateNodeModalVisible(true)}
                />
            )}
        </MainContentWrapper>
    );
};

export default GlossaryContentProvider;
