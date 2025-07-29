import { Avatar, Button } from '@components';
import { Skeleton } from 'antd';
import React, { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import { StyledLink } from '@app/actionrequestV2/item/styledComponents';
import { WorkflowRequestFormModalMode } from '@app/workflows';
import { WorkflowFormModal } from '@app/workflows/components/WorkflowFormModal';
import { useListActionWorkflows } from '@app/workflows/hooks/useListActionWorkflows';
import { convertWorkflowRequestFieldsToFormData } from '@app/workflows/utils/fieldValueConversion';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';

import { ActionRequest, ActionWorkflowEntrypointType, EntityType } from '@types';

const StyledSkeleton = styled(Skeleton.Input)`
    border-radius: 4px;
`;

type Props = {
    actionRequest: ActionRequest;
};

const ContentContainer = styled.span`
    gap: 8px;
    font-size: 14px;
    font-weight: 500;
    display: inline-flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 4px;
`;

const WorkflowName = styled.span`
    font-weight: 700;
`;

const ViewDetailsButton = styled(Button)`
    padding: 0;
    margin-left: 4px;
    height: auto;
    :hover {
        text-decoration: underline;
    }
`;

/**
 * A V2 workflow request item for the proposals table content column.
 */
export default function WorkflowFormRequestItem({ actionRequest }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const [showDetailsModal, setShowDetailsModal] = useState(false);

    const workflowFormRequest = actionRequest.params?.workflowFormRequest;

    // Memoize context to prevent recreating on every render
    const context = useMemo(
        () => ({
            entrypointType: ActionWorkflowEntrypointType.Home,
        }),
        [],
    );

    // Load workflows with cache-first strategy for optimal performance
    const { workflows, loading } = useListActionWorkflows({
        context,
        enabled: !!workflowFormRequest?.workflowUrn,
        fetchPolicy: 'cache-first', // Cache sharing is a feature, not a bug
    });

    // Find the specific workflow for this request - simplified logic
    const selectedWorkflow = useMemo(() => {
        if (!workflows.length || !workflowFormRequest?.workflowUrn) {
            return null;
        }
        const found = workflows.find((w) => w.urn === workflowFormRequest.workflowUrn);
        return found || null;
    }, [workflows, workflowFormRequest?.workflowUrn]);

    // Memoize the modal data to ensure fresh computation for each request
    const modalInitialFormData = useMemo(
        () => ({
            description: actionRequest.description || '',
            expiresAt: workflowFormRequest?.access?.expiresAt || undefined,
            fieldValues: workflowFormRequest?.fields
                ? convertWorkflowRequestFieldsToFormData(workflowFormRequest.fields)
                : {},
        }),
        [actionRequest.description, workflowFormRequest?.access?.expiresAt, workflowFormRequest?.fields],
    );

    const modalReviewContext = useMemo(
        () => ({
            currentStep: workflowFormRequest?.stepState?.stepId,
            requestUrn: actionRequest.urn,
            createdBy: actionRequest.created?.actor?.username || 'Unknown User',
            createdAt: actionRequest.created?.time,
            createdActor: actionRequest.created?.actor,
        }),
        [workflowFormRequest?.stepState?.stepId, actionRequest.urn, actionRequest.created],
    );

    // Early return after all hooks
    if (!workflowFormRequest) {
        return null;
    }

    const handleViewDetails = (e: any) => {
        e.stopPropagation();

        if (selectedWorkflow) {
            // We have the workflow data, show modal directly
            setShowDetailsModal(true);
        }
    };

    // Use the selected workflow name, or fall back to request type
    const workflowDisplayName = selectedWorkflow?.name;

    const getUserDisplayName = () => {
        const actor = actionRequest.created?.actor;
        if (actor) {
            return entityRegistry.getDisplayName(EntityType.CorpUser, actor);
        }
        return 'Unknown User';
    };

    const getUserAvatarUrl = () => {
        const actor = actionRequest.created?.actor;
        return actor?.editableInfo?.pictureLink || actor?.editableProperties?.pictureLink;
    };

    const renderAssetLink = () => {
        if (!actionRequest.entity?.type || !actionRequest.entity?.urn) {
            return null;
        }

        const entityDisplayName = entityRegistry.getDisplayName(actionRequest.entity.type, actionRequest.entity);
        return (
            <>
                {' for '}
                <StyledLink
                    to={`/${entityRegistry.getPathName(actionRequest.entity.type)}/${actionRequest.entity.urn}`}
                >
                    {entityDisplayName}.
                </StyledLink>
            </>
        );
    };

    return (
        <>
            <ContentContainer>
                <Link
                    to={`${entityRegistry.getEntityUrl(EntityType.CorpUser, actionRequest.created?.actor?.urn || '')}`}
                >
                    <Avatar
                        name={getUserDisplayName()}
                        imageUrl={getUserAvatarUrl()}
                        size="sm"
                        type={AvatarType.user}
                        showInPill
                    />
                </Link>
                created{' '}
                <WorkflowName>
                    {workflowDisplayName ||
                        (loading ? <StyledSkeleton size="small" active /> : 'unknown (deleted workflow)')}
                </WorkflowName>{' '}
                request{renderAssetLink()}
                {selectedWorkflow ? (
                    <ViewDetailsButton
                        variant="text"
                        color="gray"
                        onClick={handleViewDetails}
                        data-testid="view-details-button"
                    >
                        View details
                    </ViewDetailsButton>
                ) : null}
            </ContentContainer>

            {showDetailsModal && selectedWorkflow && (
                <WorkflowFormModal
                    key={actionRequest.urn} // Force remount when switching between different requests
                    workflow={selectedWorkflow}
                    entityUrn={actionRequest.entity?.urn}
                    open={showDetailsModal}
                    onClose={() => setShowDetailsModal(false)}
                    mode={WorkflowRequestFormModalMode.REVIEW}
                    initialFormData={modalInitialFormData}
                    reviewContext={modalReviewContext}
                />
            )}
        </>
    );
}
