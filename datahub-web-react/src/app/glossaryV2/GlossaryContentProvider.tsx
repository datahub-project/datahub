import { FileTextOutlined, FolderOutlined, UploadOutlined } from '@ant-design/icons';
import { Button, Dropdown } from '@components';
import { MenuProps } from 'antd';
import React from 'react';
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
    setIsCreateTermModalVisible: React.Dispatch<React.SetStateAction<boolean>>;
    hasTermsOrNodes: boolean;
    nodes: (GlossaryNode | GlossaryNodeFragment)[];
    terms: (GlossaryTerm | ChildGlossaryTermFragment)[];
    termsLoading: boolean;
    nodesLoading: boolean;
}

const GlossaryContentProvider = (props: Props) => {
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
            label: 'Create Glossary',
            icon: <FolderOutlined />,
            onClick: () => setIsCreateNodeModalVisible(true),
        },
        {
            key: 'create-term',
            label: 'Create Term',
            icon: <FileTextOutlined />,
            onClick: () => setIsCreateTermModalVisible(true),
        },
        {
            key: 'import',
            label: 'Import CSV',
            icon: <UploadOutlined />,
            onClick: () => history.push(PageRoutes.GLOSSARY_IMPORT),
        },
    ];

    return (
        <MainContentWrapper data-testid="glossary-entities-list">
            <HeaderWrapper data-testid="glossaryPageV2">
                <PageTitle
                    title="Business Glossary"
                    subTitle="Classify your data assets and columns using data dictionaries"
                />
                <ButtonContainer>
                    <Dropdown menu={{ items: dropdownItems }} trigger={['click']} placement="bottomRight">
                        <Button
                            id="create-glossary-object-button"
                            data-testid="create-glossary-object-button"
                            name="Create"
                            size="md"
                            icon={{ icon: 'Add', source: 'material' }}
                            // can not be disabled on acryl-main due to ability to propose
                        >
                            Create Glossary
                        </Button>
                    </Dropdown>
                </ButtonContainer>
            </HeaderWrapper>
            <ListWrapper>
                {hasTermsOrNodes && <GlossaryEntitiesList nodes={nodes || []} terms={terms || []} />}
            </ListWrapper>
            {!(termsLoading || nodesLoading) && !hasTermsOrNodes && (
                <EmptyGlossarySection
                    title="Empty Glossary"
                    description="Create Terms and Term Groups to organize data assets using a shared vocabulary."
                    onAddTerm={() => setIsCreateTermModalVisible(true)}
                    onAddtermGroup={() => setIsCreateNodeModalVisible(true)}
                />
            )}
        </MainContentWrapper>
    );
};

export default GlossaryContentProvider;
