import { LoadingOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { LockSimpleOpen } from '@phosphor-icons/react';
import { Dropdown, Menu } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { ActionMenuItem } from '@app/entityV2/shared/EntityDropdown/styledComponents';
import { WorkflowFormModal } from '@app/workflows/components/WorkflowFormModal';
import {
    getEntityProfileWorkflowContext,
    getEntryPointLabel,
    useListActionWorkflows,
} from '@app/workflows/hooks/useListActionWorkflows';
import { useIsWorkflowsEnabled } from '@app/workflows/useIsWorkflowsEnabled';

const StyledMenuItem = styled(Menu.Item)`
    padding: 4px 4px;
`;

const MenuItem = styled.div`
    font-size: 13px;
    font-weight: 400;
    padding: 4px 4px;
    color: #46507b;
    line-height: 24px;
    display: flex;
    align-items: center;
    gap: 6px;
`;

export const WorkflowsMenuAction = () => {
    const { urn, entityType } = useEntityData();
    const isWorkflowsEnabled = useIsWorkflowsEnabled();
    const context = useMemo(() => getEntityProfileWorkflowContext(entityType, urn), [entityType, urn]);

    const { workflows, loading } = useListActionWorkflows({
        context,
        enabled: isWorkflowsEnabled,
    });

    const [selectedWorkflow, setSelectedWorkflow] = useState<any | null>(null);
    const [isModalOpen, setIsModalOpen] = useState(false);

    if (!isWorkflowsEnabled || loading || (!loading && workflows.length === 0)) {
        return null;
    }

    const handleWorkflowClick = (workflow: any) => {
        setSelectedWorkflow(workflow);
        setIsModalOpen(true);
    };

    const handleModalClose = () => {
        setIsModalOpen(false);
        setSelectedWorkflow(null);
    };

    const handleWorkflowSuccess = (requestUrn: string) => {
        console.log('Workflow request submitted:', requestUrn);
    };

    const dropdownMenu = (
        <Menu>
            {loading ? (
                <Menu.Item key="loading" disabled>
                    <LoadingOutlined />
                </Menu.Item>
            ) : (
                workflows.map((workflow) => (
                    <StyledMenuItem
                        key={workflow.urn}
                        onClick={() => handleWorkflowClick(workflow)}
                        data-testid={`workflow-dropdown-item-${workflow.urn.split(':').pop()}`}
                    >
                        <MenuItem>{getEntryPointLabel(workflow, context)}</MenuItem>
                    </StyledMenuItem>
                ))
            )}
        </Menu>
    );

    return (
        <>
            <Tooltip title="Request access or start a workflow">
                <Dropdown overlay={dropdownMenu} trigger={['click']} placement="bottomRight">
                    <ActionMenuItem type="text" data-testid="workflows-menu-action">
                        <LockSimpleOpen size={16} />
                    </ActionMenuItem>
                </Dropdown>
            </Tooltip>

            {selectedWorkflow && (
                <WorkflowFormModal
                    workflow={selectedWorkflow}
                    entityUrn={urn}
                    open={isModalOpen}
                    onClose={handleModalClose}
                    onSuccess={handleWorkflowSuccess}
                />
            )}
        </>
    );
};
