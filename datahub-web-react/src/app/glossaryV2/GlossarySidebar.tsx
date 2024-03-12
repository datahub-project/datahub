import React, { useEffect, useState } from 'react';
import { Button, Typography } from 'antd';
import { PlusCircleOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import GlossarySearch from './GlossarySearch';
import GlossaryBrowser from './GlossaryBrowser/GlossaryBrowser';
import { SidebarWrapper } from '../sharedV2/sidebar/components';
import CreateGlossaryEntityModal from '../entityV2/shared/EntityDropdown/CreateGlossaryEntityModal';
import { EntityType } from '../../types.generated';
import { useUserContext } from '../context/useUserContext';
import { useGetRootGlossaryNodesQuery } from '../../graphql/glossary.generated';

const SidebarTitleWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 15px 12px 10px 15px;
    border-bottom: 1px solid ${REDESIGN_COLORS.BORDER_3};
    height: 50px;
`;

const SidebarTitle = styled(Typography)`
    font-size: 12px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.BACKGROUND_OVERLAY_BLACK};
`;

const StyledButton = styled(Button)`
    padding: 0;
    border: none;
    box-shadow: none;
    height: auto;
`;

const StyledPlusCircleOutlined = styled(PlusCircleOutlined)`
    font-size: 20px !important;
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
`;

export default function GlossarySidebar() {
    const [browserWidth, setBrowserWidth] = useState(window.innerWidth * 0.2);
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);

    const { refetch: refetchForNodes } = useGetRootGlossaryNodesQuery();

    const user = useUserContext();
    const canManageGlossaries = user?.platformPrivileges?.manageGlossaries;

    useEffect(() => {
        const handleResize = () => {
            setBrowserWidth(window.innerWidth * 0.2);
        };

        window.addEventListener('resize', handleResize);

        return () => {
            window.removeEventListener('resize', handleResize);
        };
    }, []);

    return (
        <>
            <SidebarWrapper width={browserWidth} data-testid="glossary-browser-sidebar">
                <SidebarTitleWrapper>
                    <SidebarTitle>Business Glossary</SidebarTitle>
                    <StyledButton onClick={() => setIsCreateNodeModalVisible(true)}>
                        <StyledPlusCircleOutlined />
                    </StyledButton>
                </SidebarTitleWrapper>
                <GlossarySearch />
                <GlossaryBrowser openToEntity />
            </SidebarWrapper>
            {isCreateNodeModalVisible && (
                <CreateGlossaryEntityModal
                    entityType={EntityType.GlossaryNode}
                    canCreateGlossaryEntity={!!canManageGlossaries}
                    onClose={() => setIsCreateNodeModalVisible(false)}
                    refetchData={refetchForNodes}
                />
            )}
        </>
    );
}
