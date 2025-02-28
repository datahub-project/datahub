import ViewInPlatform from '@app/entityV2/shared/externalUrl/ViewInPlatform';
import React, { useContext } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import EntitySidebarContext from '../../../../../sharedV2/EntitySidebarContext';
import { REDESIGN_COLORS } from '../../../constants';
import { EntitySidebarTab } from '../../../types';
import { TitleAction } from './TitleAction';
import MoreOptionsMenuAction from '../../../EntityDropdown/MoreOptionsMenuAction';
import { EntityMenuItems } from '../../../EntityDropdown/EntityMenuActions';
import { useEntityData, useRefetch } from '../../../../../entity/shared/EntityContext';

const Controls = styled.div<{ isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.isCollapsed ? 'center' : 'space-between')};
    height: 56px;
    padding: 8px 20px 5px 20px;
    border-bottom: 1px solid #d5d5d5;
`;

const Title = styled.div`
    display: flex;
    flex-direction: column;
    align-items: start;
    justify-content: center;
    gap: 2px;
    width: 100%;
`;

const TabTitle = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 800;
    line-height: 20px;
    color: ${REDESIGN_COLORS.HEADING_COLOR};
`;

const TitleDescription = styled(Typography.Text)`
    font-size: 10px;
    font-weight: 400;
    color: ${REDESIGN_COLORS.HEADING_COLOR};
    opacity: 0.5;
`;

const Top = styled.div`
    display: flex;
    justify-content: space-between;
    width: 100%;
`;

const RightActions = styled.div`
    display: flex;
    align-items: center;
    gap: 3px;
`;

interface Props {
    currentTab?: EntitySidebarTab;
    headerDropdownItems?: Set<EntityMenuItems>;
}

export default function SidebarCollapsibleHeader({ currentTab, headerDropdownItems }: Props) {
    const { isClosed, forLineage, separateSiblings } = useContext(EntitySidebarContext);

    const currentTabName = currentTab?.name === 'About' ? 'Summary' : currentTab?.name;
    const currentTabDescription = currentTab?.description;
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

                    {currentTabDescription && <TitleDescription> {currentTabDescription}</TitleDescription>}
                </Title>
            )}
        </Controls>
    );
}
