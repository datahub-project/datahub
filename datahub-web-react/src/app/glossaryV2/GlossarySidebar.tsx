import { Button, Tooltip } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { useUserContext } from '@app/context/useUserContext';
import CreateGlossaryEntityModal from '@app/entityV2/shared/EntityDropdown/CreateGlossaryEntityModal';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import GlossaryBrowser from '@app/glossaryV2/GlossaryBrowser/GlossaryBrowser';
import GlossarySearch from '@app/glossaryV2/GlossarySearch';
import { SidebarWrapper } from '@app/sharedV2/sidebar/components';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

import { useGetRootGlossaryNodesQuery } from '@graphql/glossary.generated';
import { EntityType } from '@types';

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
