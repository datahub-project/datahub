import React, { useState } from 'react';
import { Button, Typography } from 'antd';
import { PlusCircleOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { Link } from 'react-router-dom';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import useSidebarWidth from '../sharedV2/sidebar/useSidebarWidth';
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
    color: ${REDESIGN_COLORS.TITLE_PURPLE};
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
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);

    const { refetch: refetchForNodes } = useGetRootGlossaryNodesQuery();

    const user = useUserContext();
    const canManageGlossaries = user?.platformPrivileges?.manageGlossaries;

    const width = useSidebarWidth(0.2);

    return (
        <>
            <SidebarWrapper width={width} data-testid="glossary-browser-sidebar">
                <SidebarTitleWrapper>
                    <Link to="/glossary">
                        <SidebarTitle>Business Glossary</SidebarTitle>
                    </Link>
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
                    canSelectParentUrn={false}
                />
            )}
        </>
    );
}
