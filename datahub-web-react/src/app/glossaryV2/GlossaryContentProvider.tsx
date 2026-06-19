import { Button, Dropdown } from '@components';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { Folder } from '@phosphor-icons/react/dist/csr/Folder';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { UploadSimple } from '@phosphor-icons/react/dist/csr/UploadSimple';
import { MenuProps } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';

import EmptyGlossarySection from '@app/glossaryV2/EmptyGlossarySection';
import GlossaryEntitiesList from '@app/glossaryV2/GlossaryEntitiesList';
import { PageRoutes } from '@conf/Global';
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

    const history = useHistory();

    const dropdownItems: MenuProps['items'] = [
        {
            key: 'create-group',
            label: t('page.createGlossary'),
            icon: <Folder />,
            onClick: () => setIsCreateNodeModalVisible(true),
        },
        {
            key: 'create-term',
            label: t('page.createTerm'),
            icon: <FileText />,
            onClick: () => setIsCreateTermModalVisible(true),
        },
        {
            key: 'import',
            label: t('page.importCsv'),
            icon: <UploadSimple />,
            onClick: () => history.push(PageRoutes.GLOSSARY_IMPORT),
        },
    ];

    return (
        <MainContentWrapper data-testid="glossary-entities-list">
            <HeaderWrapper data-testid="glossaryPageV2">
                <PageTitle title={t('page.title')} subTitle={t('page.subtitle')} />
                <ButtonContainer>
                    <Dropdown menu={{ items: dropdownItems }} trigger={['click']} placement="bottomRight">
                        <Button
                            id="create-glossary-object-button"
                            data-testid="create-glossary-object-button"
                            name="Create"
                            size="md"
                            icon={{ icon: Plus }}
                            // can not be disabled on acryl-main due to ability to propose
                        >
                            {t('page.createGlossary')}
                        </Button>
                    </Dropdown>
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
