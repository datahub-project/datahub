import { useApolloClient } from '@apollo/client';
import { Form, message } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import StopPropagationWrapper from '@app/sharedV2/StopPropagationWrapper';
import ResultNote from '@app/taskCenterV2/proposalsV2/proposalsTable/ResultNote';
import { updateActionRequestsList } from '@app/taskCenterV2/proposalsV2/proposalsTable/cacheUtils';
import { ProposalModalType } from '@app/taskCenterV2/proposalsV2/utils';
import { Icon, Modal, Pill, TextArea, colors } from '@src/alchemy-components';
import analytics, { EntityActionType, EventType } from '@src/app/analytics';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useAcceptProposalsMutation, useRejectProposalsMutation } from '@src/graphql/actionRequest.generated';
import { ActionRequest, ActionRequestResult, ActionRequestStatus, EntityType } from '@src/types.generated';

const IconsContainer = styled.div`
    display: flex;
    justify-content: end;
    gap: 8px;
`;

const CompletedContainer = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
    justify-content: end;
`;

const StyledIcon = styled(Icon)`
    border-radius: 50%;
    padding: 3px;

    :hover {
        cursor: pointer;
    }
`;

type ModalType = ProposalModalType | null;

const modalConfig = {
    [ProposalModalType.Accept]: {
        title: 'Approve Proposal Note',
        subtitle: 'Please provide a reason for approving changes...',
        placeholder: 'Why are you approving the proposal?',
    },
    [ProposalModalType.Reject]: {
        title: 'Reject Proposal Note',
        subtitle: 'Please provide a reason for rejecting changes...',
        placeholder: 'Why are you rejecting the proposal?',
    },
};

interface Props {
    actionRequest: ActionRequest;
    onUpdate: (completedUrns: string[]) => void;
    showPendingView?: boolean;
}

const ActionsColumn = ({ actionRequest, onUpdate, showPendingView }: Props) => {
    const entityRegistry = useEntityRegistryV2();
    const [note, setNote] = useState('');

    const [acceptProposalsMutation] = useAcceptProposalsMutation();
    const [rejectProposalsMutation] = useRejectProposalsMutation();

    const [modalType, setModalType] = useState<ModalType>(null);

    const client = useApolloClient();

    const authenticatedUser = useUserContext();
    const currentUser = authenticatedUser?.user;

    const acceptRequest = () => {
        acceptProposalsMutation({
            variables: { urns: [actionRequest.urn], note },
        })
            .then(() => {
                if (actionRequest.entity?.urn) {
                    analytics.event({
                        type: EventType.EntityActionEvent,
                        actionType: EntityActionType.ProposalAccepted,
                        actionQualifier: actionRequest.type,
                        entityType: actionRequest.entity?.type,
                        entityUrn: actionRequest.entity?.urn,
                    });
                }
                message.success('Successfully accepted the proposal!');
                updateActionRequestsList(client, [actionRequest.urn], ActionRequestResult.Accepted, note, currentUser);
                onUpdate([actionRequest.urn]);
            })
            .catch((err) => {
                console.log(err);
                message.error('Failed to accept proposal. An unknown error occurred!');
            })
            .finally(() => {
                setModalType(null);
            });
    };

    const rejectRequest = () => {
        rejectProposalsMutation({
            variables: { urns: [actionRequest.urn], note },
        })
            .then(() => {
                if (actionRequest.entity?.urn) {
                    analytics.event({
                        type: EventType.EntityActionEvent,
                        actionType: EntityActionType.ProposalRejected,
                        actionQualifier: actionRequest.type,
                        entityType: actionRequest.entity?.type,
                        entityUrn: actionRequest.entity?.urn,
                    });
                }
                message.info('Proposal declined.');
                updateActionRequestsList(client, [actionRequest.urn], ActionRequestResult.Rejected, note, currentUser);
                onUpdate([actionRequest.urn]);
            })
            .catch((err) => {
                console.log(err);
                message.error('Failed to reject proposal. An unknown error occurred!');
            })
            .finally(() => {
                setModalType(null);
            });
    };

    let actionResultView;

    if (actionRequest.status === ActionRequestStatus.Completed) {
        const resultAuthor = actionRequest.lastModified?.actor; // Who completed the request.
        const isAccepted = actionRequest.result === ActionRequestResult.Accepted;
        const resultAuthorDisplayName =
            resultAuthor && entityRegistry.getDisplayName(EntityType.CorpUser, resultAuthor);
        actionResultView = (
            <CompletedContainer>
                {actionRequest.resultNote && (
                    <ResultNote resultNote={actionRequest.resultNote} authorDisplayName={resultAuthorDisplayName} />
                )}
                <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${resultAuthor?.urn}`}>
                    <Pill
                        leftIcon={isAccepted ? 'Check' : 'Close'}
                        label={resultAuthorDisplayName || ''}
                        color={isAccepted ? 'green' : 'red'}
                        size="md"
                    />
                </Link>
            </CompletedContainer>
        );
    } else if (showPendingView) {
        // Just showing the status until we support deleting a proposal
        actionResultView = (
            <Pill
                size="md"
                customIconRenderer={() => <Icon size="md" icon="Spinner" source="phosphor" />}
                label="Pending"
            />
        );
    } else {
        actionResultView = (
            <IconsContainer>
                <StyledIcon
                    icon="Close"
                    color="red"
                    style={{ backgroundColor: colors.red[0] }}
                    onClick={(e) => {
                        e.stopPropagation();
                        setModalType(ProposalModalType.Reject);
                    }}
                    data-testid="decline-button"
                />
                <StyledIcon
                    icon="Check"
                    color="green"
                    style={{ backgroundColor: colors.green[0] }}
                    onClick={(e) => {
                        e.stopPropagation();
                        setModalType(ProposalModalType.Accept);
                    }}
                    data-testid="approve-button"
                />
            </IconsContainer>
        );
    }

    const handleCancel = () => {
        setModalType(null);
        setNote('');
    };

    return (
        <>
            {actionResultView}
            {!!modalType && (
                <StopPropagationWrapper>
                    <Modal
                        buttons={[
                            {
                                text: 'Cancel',
                                variant: 'text',
                                onClick: handleCancel,
                            },
                            {
                                text: 'Submit',
                                variant: 'filled',
                                onClick: modalType === ProposalModalType.Accept ? acceptRequest : rejectRequest,
                            },
                        ]}
                        onCancel={handleCancel}
                        title={modalConfig[modalType].title}
                        subtitle={modalConfig[modalType].subtitle}
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
                </StopPropagationWrapper>
            )}
        </>
    );
};

export default ActionsColumn;
