import { Button, Tooltip, colors } from '@components';
import { ArrowRight } from '@phosphor-icons/react';
import { Skeleton } from 'antd';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { ReferenceSection } from '@app/homeV2/layout/shared/styledComponents';
import { ReferenceSectionProps } from '@app/homeV2/reference/types';
import { WorkflowFormModal } from '@app/workflows/components/WorkflowFormModal';
import {
    getEntryPointLabel,
    getHomePageWorkflowContext,
    useListActionWorkflows,
} from '@app/workflows/hooks/useListActionWorkflows';
import { useIsWorkflowsEnabled } from '@app/workflows/useIsWorkflowsEnabled';

import { ActionWorkflowFragment } from '@graphql/actionWorkflow.generated';

const WorkflowsContainer = styled.div`
    margin-bottom: 16px;
`;

const SectionTitle = styled.div`
    font-size: 16px;
    font-weight: 600;
    color: #262626;
    margin-bottom: 8px;
    cursor: pointer;
`;

const WorkflowItem = styled.div`
    display: flex;
    align-items: flex-start;
    gap: 8px;
    margin-left: -8px;
    padding: 4px 8px;
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
`;

const EmptyState = styled.div`
    padding: 16px;
    text-align: center;
    color: ${colors.gray[500]};
    font-size: 13px;
`;

const ShowMoreButton = styled(Button)`
    width: 100%;
    margin-top: 8px;
    padding: 4px 2px;
`;

const DEFAULT_MAX_WORKFLOWS_TO_SHOW = 3;

export const WorkflowsYouCanStart = ({ hideIfEmpty, trackClickInSection }: ReferenceSectionProps) => {
    const isWorkflowsEnabled = useIsWorkflowsEnabled();

    const context = useMemo(() => getHomePageWorkflowContext(), []);
    const { workflows, loading, error } = useListActionWorkflows({
        context,
        enabled: true,
    });

    const [selectedWorkflow, setSelectedWorkflow] = useState<ActionWorkflowFragment | null>(null);
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [showAllWorkflows, setShowAllWorkflows] = useState(false);

    if (!isWorkflowsEnabled || loading || (hideIfEmpty && !loading && workflows.length === 0)) {
        return null;
    }

    const handleWorkflowClick = (workflow: ActionWorkflowFragment) => {
        setSelectedWorkflow(workflow);
        setIsModalOpen(true);
        trackClickInSection?.(workflow.urn);
    };

    const handleModalClose = () => {
        setIsModalOpen(false);
        setSelectedWorkflow(null);
    };

    const handleWorkflowSuccess = (requestUrn: string) => {
        // Could add success notification here
        console.log('Workflow request submitted:', requestUrn);
    };

    if (error) {
        console.error('Error loading workflows:', error);
        return null;
    }

    const displayedWorkflows = showAllWorkflows ? workflows : workflows.slice(0, DEFAULT_MAX_WORKFLOWS_TO_SHOW);

    const hasMoreWorkflows = workflows.length > DEFAULT_MAX_WORKFLOWS_TO_SHOW && !showAllWorkflows;

    return (
        <ReferenceSection>
            <WorkflowsContainer data-testid="workflows-you-can-start">
                <Tooltip title="Request access, report issues, or submit other forms">
                    <SectionTitle data-testid="workflows-section-title">Start a workflow</SectionTitle>
                </Tooltip>

                {loading && <Skeleton active />}

                {!loading && workflows.length === 0 && (
                    <EmptyState data-testid="workflows-empty-state">No workflows available</EmptyState>
                )}

                {!loading && workflows.length > 0 && (
                    <>
                        {displayedWorkflows.map((workflow) => (
                            <WorkflowItem
                                key={workflow.urn}
                                onClick={() => handleWorkflowClick(workflow)}
                                data-testid={`workflow-item-${workflow.urn.split(':').pop()}`}
                            >
                                <WorkflowName data-testid={`workflow-name-${workflow.urn.split(':').pop()}`}>
                                    {getEntryPointLabel(workflow, context)}
                                    <ArrowRight />
                                </WorkflowName>
                            </WorkflowItem>
                        ))}

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
            </WorkflowsContainer>

            {selectedWorkflow && (
                <WorkflowFormModal
                    workflow={selectedWorkflow}
                    open={isModalOpen}
                    onClose={handleModalClose}
                    onSuccess={handleWorkflowSuccess}
                />
            )}
        </ReferenceSection>
    );
};
