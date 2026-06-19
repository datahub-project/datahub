import { Button, Dropdown, Tooltip } from '@components';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { Folder } from '@phosphor-icons/react/dist/csr/Folder';
import { UploadSimple } from '@phosphor-icons/react/dist/csr/UploadSimple';
import { MenuProps } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useUserContext } from '@app/context/useUserContext';
import CreateGlossaryEntityModal from '@app/entityV2/shared/EntityDropdown/CreateGlossaryEntityModal';
import GlossaryBrowser from '@app/glossaryV2/GlossaryBrowser/GlossaryBrowser';
import GlossarySearch from '@app/glossaryV2/GlossarySearch';
import { SidebarWrapper } from '@app/sharedV2/sidebar/components';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';

import { useGetRootGlossaryNodesQuery } from '@graphql/glossary.generated';
import { EntityType } from '@types';

const StyledSidebarWrapper = styled(SidebarWrapper)<{ $isEntityProfile?: boolean }>`
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
 margin: ${props.$isEntityProfile ? '5px 0px 6px 5px' : '0px 4px 0px 0px'};
 `}
    padding-bottom: 16px;
`;

const SidebarTitleWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
    height: 50px;
    font-size: 20px;
`;

const GlossaryTitle = styled.div`
    font-size: 16px;
    font-weight: bold;
    color: ${(props) => props.theme.colors.text};
`;

const StyledButton = styled(Button)`
    padding: 2px;
    svg {
        width: 20px;
        height: 20px;
    }
`;

type Props = {
    isEntityProfile?: boolean;
};

export default function GlossarySidebar({ isEntityProfile }: Props) {
    const { t } = useTranslation('governance.glossary');
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);
    const [isCreateTermModalVisible, setIsCreateTermModalVisible] = useState(false);

    const { refetch: refetchForNodes } = useGetRootGlossaryNodesQuery();
    const history = useHistory();

    const user = useUserContext();
    const canManageGlossaries = user?.platformPrivileges?.manageGlossaries;

    const width = useSidebarWidth(0.2);
    const isShowNavBarRedesign = useShowNavBarRedesign();

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
        <>
            <StyledSidebarWrapper
                width={width}
                data-testid="glossary-browser-sidebar"
                $isShowNavBarRedesign={isShowNavBarRedesign}
                $isEntityProfile={isEntityProfile}
            >
                <SidebarTitleWrapper>
                    <GlossaryTitle>{t('page.title')}</GlossaryTitle>
                    <Tooltip title={t('page.glossaryActions')} placement="left" showArrow={false}>
                        <Dropdown menu={{ items: dropdownItems }} trigger={['click']} placement="bottomRight">
                            <StyledButton
                                id="create-glossary-object-button-sidebar"
                                data-testid="create-glossary-object-button-sidebar"
                                variant="text"
                                color="gray"
                                icon={{ icon: DotsThreeVertical }}
                            />
                        </Dropdown>
                    </Tooltip>
                </SidebarTitleWrapper>
                <GlossarySearch />
                <GlossaryBrowser openToEntity />
            </StyledSidebarWrapper>
            {isCreateNodeModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryNode}
                    canCreateGlossaryEntity={!!canManageGlossaries}
                    onClose={() => setIsCreateNodeModalVisible(false)}
                    refetchData={refetchForNodes}
                    canSelectParentUrn={false}
                />
            )}
            {isCreateTermModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryTerm}
                    canCreateGlossaryEntity={!!canManageGlossaries}
                    onClose={() => setIsCreateTermModalVisible(false)}
                    refetchData={refetchForNodes}
                />
            )}
        </>
    );
}
