import { ListChecks } from '@phosphor-icons/react';
import { Tooltip, Typography } from 'antd';
import React, { useContext, useState } from 'react';
import styled from 'styled-components';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import MoreOptionsMenuAction from '@app/entityV2/shared/EntityDropdown/MoreOptionsMenuAction';
import { TitleAction } from '@app/entityV2/shared/containers/profile/sidebar/TitleAction';
import ViewInPlatform from '@app/entityV2/shared/externalUrl/ViewInPlatform';
import { EntitySidebarTab } from '@app/entityV2/shared/types';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import { Modal, colors } from '@src/alchemy-components';
import { ProposalList } from '@src/app/taskCenterV2/proposalsV2/ProposalList';
import { entityHasProposals } from '@src/app/taskCenterV2/proposalsV2/utils';
import { useAppConfig } from '@src/app/useAppConfig';

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

const TasksIcon = styled.span`
    display: flex;
    position: relative;
    cursor: pointer;
    margin-right: 4px;
`;

const PillDot = styled.div<{ $isSelected?: boolean }>`
    position: absolute;
    width: 10px;
    height: 10px;
    background: ${(props) => props.theme.styles['primary-color']};
    border-radius: 6px;
    border: 2px solid ${(props) => (props.$isSelected ? '#f9fafc' : '#f2f3fa')};
    top: -2px;
    left: 14px;
`;

interface Props {
    currentTab?: EntitySidebarTab;
    headerDropdownItems?: Set<EntityMenuItems>;
}

export default function SidebarCollapsibleHeader({ currentTab, headerDropdownItems }: Props) {
    const { isClosed, forLineage, separateSiblings } = useContext(EntitySidebarContext);
    const { config } = useAppConfig();
    const { showTaskCenterRedesign } = config.featureFlags;
    const [showProposalsModal, setShowProposalsModal] = useState(false);

    const currentTabName = currentTab?.name;
    const actionType = currentTab?.properties?.actionType;
    const icon = currentTab?.icon;

    const { urn, entityType, entityData } = useEntityData();
    const refetch = useRefetch();

    const handleModalClose = () => {
        setShowProposalsModal(false);
        refetch();
    };

    return (
        <Controls isCollapsed={isClosed}>
            {!isClosed && currentTab && (
                <Title>
                    <Top>
                        <TabTitle>{currentTabName}</TabTitle>
                        <RightActions>
                            {showTaskCenterRedesign && entityHasProposals(entityData) && (
                                <Tooltip title="Tasks" placement="right">
                                    <TasksIcon>
                                        <ListChecks onClick={() => setShowProposalsModal(true)} size={20} />
                                        {/* Always show the PillDot for now */}
                                        <PillDot />
                                    </TasksIcon>
                                </Tooltip>
                            )}
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
                    {showProposalsModal && (
                        // TODO: Add Proposals count Badge in the Modal title
                        <Modal width="90%" title="Proposals" onCancel={handleModalClose}>
                            <ProposalList resourceUrn={urn} height="700px" />
                        </Modal>
                    )}
                </Title>
            )}
        </Controls>
    );
}
