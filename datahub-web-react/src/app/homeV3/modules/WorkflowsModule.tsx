import { Button, colors } from '@components';
import { ArrowRight } from '@phosphor-icons/react';
import { Skeleton } from 'antd';
import React, { useCallback, useMemo, useState } from 'react';
import styled from 'styled-components';

import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import { WorkflowFormModal } from '@app/workflows/components/WorkflowFormModal';
import {
    getEntryPointLabel,
    getHomePageWorkflowContext,
    useListActionWorkflows,
} from '@app/workflows/hooks/useListActionWorkflows';

import { ActionWorkflowFragment } from '@graphql/actionWorkflow.generated';
import { ActionWorkflowCategory, EntityType } from '@types';

const WorkflowItem = styled.div`
    display: flex;
    align-items: flex-start;
    gap: 8px;
    margin-left: 0px;
    padding: 8px;
    border-radius: 8px;
    cursor: pointer;
    transition: background-color 0.2s ease;

    &:hover {
        background-color: ${colors.gray[1500]};
    }
`;

const WorkflowName = styled.div`
    font-size: 14px;
    font-weight: 500;
    color: ${colors.primary[500]};
    margin-bottom: 2px;
    display: flex;
    align-items: center;
    gap: 4px;
    flex: 1;
`;

const ShowMoreButton = styled(Button)`
    width: 100%;
    margin-top: 8px;
    padding: 4px 2px;
`;

const DEFAULT_MAX_WORKFLOWS_TO_SHOW = 3;

export default function WorkflowsModule(props: ModuleProps) {
    const context = useMemo(() => getHomePageWorkflowContext(), []);
    const { workflows, loading, error } = useListActionWorkflows({
        context,
        enabled: true, // If the module is rendered, we are assuming workflows are enabled.
    });

    const [selectedWorkflow, setSelectedWorkflow] = useState<ActionWorkflowFragment | null>(null);
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [showAllWorkflows, setShowAllWorkflows] = useState(false);

    const handleWorkflowClick = useCallback((workflow: ActionWorkflowFragment) => {
        setSelectedWorkflow(workflow);
        setIsModalOpen(true);
    }, []);

    const handleModalClose = useCallback(() => {
        setIsModalOpen(false);
        setSelectedWorkflow(null);
    }, []);

    const handleWorkflowSuccess = useCallback(
        (requestUrn: string) => {
            console.log('Workflow request submitted:', requestUrn);
            handleModalClose();
        },
        [handleModalClose],
    );

    if (error) {
        console.error('Error loading workflows:', error);
        return null;
    }

    const displayedWorkflows = showAllWorkflows ? workflows : workflows.slice(0, DEFAULT_MAX_WORKFLOWS_TO_SHOW);
    const hasMoreWorkflows = workflows.length > DEFAULT_MAX_WORKFLOWS_TO_SHOW && !showAllWorkflows;

    const renderWorkflow = (workflow: ActionWorkflowFragment) => {
        return (
            <WorkflowItem
                key={workflow.urn}
                onClick={() => handleWorkflowClick(workflow)}
                data-testid={`workflow-item-${workflow.urn.split(':').pop()}`}
            >
                <WorkflowName data-testid={`workflow-name-${workflow.urn.split(':').pop()}`}>
                    {getEntryPointLabel(workflow, context)}
                    <ArrowRight size={16} />
                </WorkflowName>
            </WorkflowItem>
        );
    };

    const renderSingleWorkflow = (workflow: ActionWorkflowFragment) => {
        if (workflow.category === ActionWorkflowCategory.Access) {
            return (
                <EmptyContent
                    dataTestId={`workflow-item-${workflow.urn.split(':').pop()}`}
                    key={workflow.urn}
                    icon="LockSimpleOpen"
                    title="Access Request"
                    description="Looking for access to a certain asset?"
                    linkText={getEntryPointLabel(workflow, context)}
                    onLinkClick={() => handleWorkflowClick(workflow)}
                />
            );
        }
        return renderWorkflow(workflow);
    };

    return (
        <>
            <LargeModule {...props} loading={loading}>
                {loading && <Skeleton active />}

                {!loading && workflows.length === 0 && (
                    <EmptyContent
                        icon="LockSimpleOpen"
                        title="No Workflows Available"
                        description="No workflows are currently available to start"
                    />
                )}

                {!loading && workflows.length > 0 && (
                    <>
                        {displayedWorkflows.length === 1 && displayedWorkflows.map(renderSingleWorkflow)}

                        {displayedWorkflows.length > 1 && displayedWorkflows.map(renderWorkflow)}

                        {hasMoreWorkflows && (
                            <ShowMoreButton
                                variant="text"
                                size="sm"
                                color="gray"
                                onClick={() => setShowAllWorkflows(true)}
                                data-testid="show-more-workflows-button"
                            >
                                Show {workflows.length - DEFAULT_MAX_WORKFLOWS_TO_SHOW} more
                            </ShowMoreButton>
                        )}
                    </>
                )}
            </LargeModule>

            {selectedWorkflow && (
                <WorkflowFormModal
                    workflow={selectedWorkflow}
                    open={isModalOpen}
                    onClose={handleModalClose}
                    onSuccess={handleWorkflowSuccess}
                />
            )}
        </>
    );
}
