import { colors } from '@components';
import { Typography } from 'antd';
import React, { useContext } from 'react';
import styled from 'styled-components';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import MoreOptionsMenuAction from '@app/entityV2/shared/EntityDropdown/MoreOptionsMenuAction';
import { TitleAction } from '@app/entityV2/shared/containers/profile/sidebar/TitleAction';
import ViewInPlatform from '@app/entityV2/shared/externalUrl/ViewInPlatform';
import { EntitySidebarTab } from '@app/entityV2/shared/types';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';

const Controls = styled.div<{ isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.isCollapsed ? 'center' : 'space-between')};
    height: 56px;
    padding: 8px 20px 5px 20px;
    border-bottom: 1px solid ${colors.gray[100]};
`;

const Title = styled.div`
    display: flex;
    flex-direction: column;
    align-items: start;
    justify-content: center;
    width: 100%;
`;

const TabTitle = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 800;
    line-height: 20px;
    color: ${colors.gray[600]};
`;

// eslint-disable-next-line @typescript-eslint/naming-convention
const _TitleDescription = styled(Typography.Text)`
    font-size: 12px;
    font-weight: 400;
    color: ${colors.gray[1700]};
`;

const Top = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
`;

const RightActions = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

interface Props {
    currentTab?: EntitySidebarTab;
    headerDropdownItems?: Set<EntityMenuItems>;
}

export default function SidebarCollapsibleHeader({ currentTab, headerDropdownItems }: Props) {
    const { isClosed, forLineage, separateSiblings } = useContext(EntitySidebarContext);

    const currentTabName = currentTab?.name;
    const actionType = currentTab?.properties?.actionType;
    const icon = currentTab?.icon;

    const { urn, entityType, entityData } = useEntityData();
    const refetch = useRefetch();

    return (
        <Controls isCollapsed={isClosed}>
            {!isClosed && currentTab && (
                <Title>
                    <Top>
                        <TabTitle>{currentTabName}</TabTitle>
                        <RightActions>
                            {actionType && <TitleAction actionType={actionType} icon={icon} />}
                            {forLineage && (
                                <ViewInPlatform hideSiblingActions={separateSiblings} urn={urn} data={entityData} />
                            )}
                            {forLineage && headerDropdownItems && (
                                <MoreOptionsMenuAction
                                    menuItems={headerDropdownItems}
                                    urn={urn}
                                    entityType={entityType}
                                    entityData={entityData}
                                    refetch={refetch}
                                    size={22}
                                />
                            )}
                        </RightActions>
                    </Top>
                </Title>
            )}
        </Controls>
    );
}
