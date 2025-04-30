import { Form, Input, message } from 'antd';
import { useForm } from 'antd/lib/form/Form';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import ResultNote from '@app/taskCenterV2/proposalsV2/proposalsTable/ResultNote';
import { ProposalModalType } from '@app/taskCenterV2/proposalsV2/utils';
import { Icon, Modal, Pill, colors } from '@src/alchemy-components';
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
const PROPOSAL_REVIEW_NOTE = 'proposalReviewNote';

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
    onUpdate: () => void;
    showPendingView?: boolean;
}

const ActionsColumn = ({ actionRequest, onUpdate, showPendingView }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const [acceptProposalsMutation] = useAcceptProposalsMutation();
    const [rejectProposalsMutation] = useRejectProposalsMutation();

    const [form] = useForm();
    const [modalType, setModalType] = useState<ModalType>(null);

    const acceptRequest = () => {
        acceptProposalsMutation({
            variables: { urns: [actionRequest.urn], note: form.getFieldValue(PROPOSAL_REVIEW_NOTE) },
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
                onUpdate();
            })
            .catch((err) => {
                console.log(err);
                message.error('Failed to accept proposal. An unknown error occurred!');
            });
    };

    const rejectRequest = () => {
        rejectProposalsMutation({
            variables: { urns: [actionRequest.urn], note: form.getFieldValue(PROPOSAL_REVIEW_NOTE) },
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
                onUpdate();
            })
            .catch((err) => {
                console.log(err);
                message.error('Failed to reject proposal. An unknown error occurred!');
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
        form.resetFields();
    };

    return (
        <>
            {actionResultView}
            {!!modalType && (
                <div
                    onClick={(e) => e.stopPropagation()}
                    role="button"
                    tabIndex={0}
                    onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                            e.stopPropagation();
                        }
                    }}
                >
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
                        <Form form={form} initialValues={{}} layout="vertical">
                            <div>
                                <div>Reason</div>
                                <Form.Item name={PROPOSAL_REVIEW_NOTE}>
                                    <Input.TextArea rows={4} placeholder={modalConfig[modalType].placeholder} />
                                </Form.Item>
                            </div>
                        </Form>
                    </Modal>
                </div>
            )}
        </>
    );
};

export default ActionsColumn;
