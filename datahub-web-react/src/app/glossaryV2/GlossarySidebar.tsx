import React, { useState } from 'react';
import { Tooltip, Button } from '@components';
import styled from 'styled-components/macro';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import useSidebarWidth from '../sharedV2/sidebar/useSidebarWidth';
import GlossarySearch from './GlossarySearch';
import GlossaryBrowser from './GlossaryBrowser/GlossaryBrowser';
import { SidebarWrapper } from '../sharedV2/sidebar/components';
import CreateGlossaryEntityModal from '../entityV2/shared/EntityDropdown/CreateGlossaryEntityModal';
import { EntityType } from '../../types.generated';
import { useUserContext } from '../context/useUserContext';
import { useGetRootGlossaryNodesQuery } from '../../graphql/glossary.generated';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';

const StyledSidebarWrapper = styled(SidebarWrapper)<{ $isEntityProfile?: boolean }>`
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        margin: ${props.$isEntityProfile ? '5px 0px 6px 5px' : '0px 4px 0px 0px'};
    `}
`;

const SidebarTitleWrapper = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px;
    border-bottom: 1px solid ${REDESIGN_COLORS.BORDER_3};
    height: 50px;
    font-size: 20px;
`;

const GlossaryTitle = styled.div`
    font-size: 16px;
    font-weight: bold;
    color: #374066;
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
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);

    const { refetch: refetchForNodes } = useGetRootGlossaryNodesQuery();

    const user = useUserContext();
    const canManageGlossaries = user?.platformPrivileges?.manageGlossaries;

    const width = useSidebarWidth(0.2);
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <>
            <StyledSidebarWrapper
                width={width}
                data-testid="glossary-browser-sidebar"
                $isShowNavBarRedesign={isShowNavBarRedesign}
                $isEntityProfile={isEntityProfile}
            >
                <SidebarTitleWrapper>
                    <GlossaryTitle>Business Glossary</GlossaryTitle>
                    <Tooltip title="Create Glossary" placement="left" showArrow={false}>
                        <StyledButton
                            variant="filled"
                            color="violet"
                            isCircle
                            icon={{ icon: 'Plus', source: 'phosphor' }}
                            onClick={() => setIsCreateNodeModalVisible(true)}
                        />
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
        </>
    );
}
