import { useApolloClient } from '@apollo/client';
import { Divider, Form, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { updateActionRequestsList } from '@app/taskCenterV2/proposalsV2/proposalsTable/cacheUtils';
import { ProposalModalType, createBatchProposalActionEvent } from '@app/taskCenterV2/proposalsV2/utils';
import { Button, Icon, Modal, Text, TextArea, colors } from '@src/alchemy-components';
import analytics, { EventType } from '@src/app/analytics';
import { useAcceptProposalsMutation, useRejectProposalsMutation } from '@src/graphql/actionRequest.generated';
import { useReviewActionWorkflowFormRequestMutation } from '@src/graphql/actionWorkflow.generated';

import { ActionRequest, ActionRequestResult, ActionRequestType } from '@types';

const ActionsContainer = styled.div<{ $hasPagination?: boolean }>`
    display: flex;
    padding: 8px;
    margin: 2px 16px 16px 16px;
    justify-content: center;
    align-items: center;
    gap: 8px;
    width: fit-content;
    align-self: center;
    border-radius: 12px;
    box-shadow: 0px 4px 12px 0px rgba(9, 1, 61, 0.12);

    background-color: white;
    position: absolute;
    left: 50%;
    bottom: ${(props) => (props.$hasPagination ? '44px' : '12px')};
    transform: translateX(-55%);
`;

const ButtonsContainer = styled.div`
    display: flex;
    gap: 8px;
`;

const SelectedContainer = styled.div`
    border-radius: 8px;
    border: 1px solid ${colors.gray[100]};
    padding: 6px 8px;
    display: flex;
    align-items: center;
    gap: 4px;
`;

const VerticalDivider = styled(Divider)`
    height: auto;
    margin: 0 4px;
    align-self: stretch;
`;

const StyledIcon = styled(Icon)`
    cursor: pointer;
`;

type ModalType = ProposalModalType | null;

const modalConfig = {
    [ProposalModalType.AcceptAll]: {
        title: 'Accept All',
        subtitle: 'Please provide a reason for approving changes...',
        placeholder: 'Why are you approving the proposals?',
    },
    [ProposalModalType.RejectAll]: {
        title: 'Reject All',
        subtitle: 'Please provide a reason for declining changes...',
        placeholder: 'Why are you declining the proposals?',
    },
};

interface Props {
    selectedUrns: string[];
    setSelectedUrns: React.Dispatch<React.SetStateAction<string[]>>;
    selectedProposals: ActionRequest[];
    onActionRequestUpdate: (completedUrns: string[]) => void;
    hasPagination?: boolean;
}

const ActionsBar = ({
    selectedUrns,
    selectedProposals,
    setSelectedUrns,
    onActionRequestUpdate,
    hasPagination,
}: Props) => {
    const [modalType, setModalType] = useState<ModalType>(null);
    const [note, setNote] = useState('');

    const [acceptProposalsMutation] = useAcceptProposalsMutation();
    const [rejectProposalsMutation] = useRejectProposalsMutation();
    const [reviewWorkflowFormRequestMutation] = useReviewActionWorkflowFormRequestMutation();

    const client = useApolloClient();

    const authenticatedUser = useUserContext();
    const currentUser = authenticatedUser?.user;

    // Separate workflow requests from regular proposals
    const workflowRequests = selectedProposals.filter(
        (proposal) => proposal.type === ActionRequestType.WorkflowFormRequest,
    );
    const regularProposals = selectedProposals.filter(
        (proposal) => proposal.type !== ActionRequestType.WorkflowFormRequest,
    );
    const workflowRequestUrns = workflowRequests.map((request) => request.urn);
    const regularProposalUrns = regularProposals.map((request) => request.urn);

    const acceptSelectedProposals = async () => {
        try {
            // Handle regular proposals with bulk mutation
            if (regularProposalUrns.length > 0) {
                await acceptProposalsMutation({
                    variables: { urns: regularProposalUrns, note },
                });

                // Analytics for regular proposals
                analytics.event({
                    type: EventType.BatchProposalActionEvent,
                    ...createBatchProposalActionEvent('ProposalsAccepted', regularProposals),
                });
            }

            // Handle workflow requests individually
            if (workflowRequestUrns.length > 0) {
                await Promise.all(
                    workflowRequestUrns.map((urn) =>
                        // TODO: Migrate to use batch mutation
                        reviewWorkflowFormRequestMutation({
                            variables: {
                                input: {
                                    urn,
                                    result: ActionRequestResult.Accepted,
                                    comment: note || undefined,
                                },
                            },
                        }),
                    ),
                );

                analytics.event({
                    type: EventType.BatchReviewActionWorkflowFormRequest,
                    actionType: ActionRequestResult.Accepted,
                    actionRequestUrns: workflowRequestUrns,
                });
            }

            message.success('Approved requests!');
            updateActionRequestsList(client, selectedUrns, ActionRequestResult.Accepted, note, currentUser);
            onActionRequestUpdate(selectedUrns);
            setSelectedUrns([]);
        } catch (err) {
            console.log(err);
            message.error('Failed to approve requests. An unexpected error occurred.');
        }
    };

    const rejectSelectedProposals = async () => {
        try {
            // Handle regular proposals with bulk mutation
            if (regularProposalUrns.length > 0) {
                await rejectProposalsMutation({
                    variables: { urns: regularProposalUrns, note },
                });

                // Analytics for regular proposals
                analytics.event({
                    type: EventType.BatchProposalActionEvent,
                    ...createBatchProposalActionEvent('ProposalsRejected', regularProposals),
                });
            }

            // Handle workflow requests individually
            if (workflowRequestUrns.length > 0) {
                await Promise.all(
                    // TODO: Migrate to use batch mutation
                    workflowRequestUrns.map((urn) =>
                        reviewWorkflowFormRequestMutation({
                            variables: {
                                input: {
                                    urn,
                                    result: ActionRequestResult.Rejected,
                                    comment: note || undefined,
                                },
                            },
                        }),
                    ),
                );

                analytics.event({
                    type: EventType.BatchReviewActionWorkflowFormRequest,
                    actionType: ActionRequestResult.Rejected,
                    actionRequestUrns: workflowRequestUrns,
                });
            }

            message.success('Requests declined.');
            updateActionRequestsList(client, selectedUrns, ActionRequestResult.Rejected, note, currentUser);
            onActionRequestUpdate(selectedUrns);
            setSelectedUrns([]);
        } catch (err) {
            console.log(err);
            message.error('Failed to reject requests. An unexpected error occurred.');
        }
    };

    const handleCancel = () => {
        setModalType(null);
        setNote('');
    };

    return (
        <ActionsContainer $hasPagination={hasPagination} data-testid="proposals-footer-actions">
            <SelectedContainer>
                <Text color="gray">{`${selectedUrns.length} Selected`}</Text>
                <StyledIcon source="phosphor" icon="X" size="md" color="gray" onClick={() => setSelectedUrns([])} />
            </SelectedContainer>
            <VerticalDivider type="vertical" />
            <ButtonsContainer>
                <Button color="red" variant="filled" onClick={() => setModalType(ProposalModalType.RejectAll)}>
                    Reject All
                </Button>
                <Button color="green" variant="filled" onClick={() => setModalType(ProposalModalType.AcceptAll)}>
                    Accept All
                </Button>
            </ButtonsContainer>
            {!!modalType && (
                <Modal
                    title={modalConfig[modalType]?.title}
                    subtitle={
                        modalType === ProposalModalType.AcceptAll
                            ? `Are you sure you want to accept these (${selectedUrns.length}) changes?`
                            : `Are you sure you want to reject these (${selectedUrns.length}) changes?`
                    }
                    onCancel={handleCancel}
                    buttons={[
                        {
                            text: 'Cancel',
                            variant: 'text',
                            onClick: handleCancel,
                        },
                        {
                            text: 'Submit',
                            onClick:
                                modalType === ProposalModalType.AcceptAll
                                    ? acceptSelectedProposals
                                    : rejectSelectedProposals,
                        },
                    ]}
                >
                    <Form>
                        <TextArea
                            label="Add Note"
                            placeholder="Note - optional"
                            value={note}
                            onChange={(e) => setNote(e.target.value)}
                        />
                    </Form>
                </Modal>
            )}
        </ActionsContainer>
    );
};

export default ActionsBar;
