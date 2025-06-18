import { Form, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useRefetch } from '@app/entity/shared/EntityContext';
import { ModalButtonContainer } from '@app/shared/button/styledComponents';
import { Button, Modal, TextArea } from '@src/alchemy-components';
import { ActionRequestListItem } from '@src/app/actionrequestV2/item/ActionRequestListItem';

import { useAcceptProposalsMutation, useRejectProposalsMutation } from '@graphql/actionRequest.generated';
import { ActionRequest } from '@types';

const ProposalModalFooter = styled.div`
    display: flex;
    justify-content: end;
`;

const StyledForm = styled(Form)`
    margin-top: 20px;
`;

type Props = {
    actionRequest: ActionRequest;
    selectedActionRequest: ActionRequest | undefined | null;
    setSelectedActionRequest: React.Dispatch<React.SetStateAction<ActionRequest | undefined | null>>;
    refetch?: () => void | Promise<any>;
};

export default function ProposalModal({
    actionRequest,
    selectedActionRequest,
    setSelectedActionRequest,
    refetch,
}: Props) {
    const [acceptProposalsMutation] = useAcceptProposalsMutation();
    const [rejectProposalsMutation] = useRejectProposalsMutation();
    const entityRefetch = useRefetch();

    const [note, setNote] = useState('');

    const handleRefetch = () => {
        if (refetch) refetch();
        entityRefetch();
    };

    const onCloseProposalDecisionModal = () => {
        setSelectedActionRequest(null);
        setTimeout(() => handleRefetch(), 3500);
    };

    const onProposalAcceptance = () => {
        acceptProposalsMutation({ variables: { urns: [actionRequest.urn], note } })
            .then(() => {
                message.success('Successfully accepted the proposal!');
                analytics.event({
                    type: EventType.EntityActionEvent,
                    actionType: EntityActionType.ProposalAccepted,
                    actionQualifier: actionRequest.type,
                    entityType: actionRequest.entity?.type,
                    entityUrn: actionRequest.entity?.urn || '',
                });
            })
            .then(handleRefetch)
            .catch((err) => {
                console.log(err);
                message.error('Failed to accept proposal. :(');
            });
    };

    const onProposalRejection = () => {
        rejectProposalsMutation({ variables: { urns: [actionRequest.urn], note } })
            .then(() => {
                message.info('Proposal declined.');
                analytics.event({
                    type: EventType.EntityActionEvent,
                    actionType: EntityActionType.ProposalRejected,
                    actionQualifier: actionRequest.type,
                    entityType: actionRequest.entity?.type,
                    entityUrn: actionRequest.entity?.urn || '',
                });
            })
            .then(handleRefetch)
            .catch((err) => {
                console.log(err);
                message.error('Failed to reject proposal. :(');
            });
    };

    return (
        <span
            role="button"
            tabIndex={0}
            onClick={(e) => e.stopPropagation()}
            onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    e.stopPropagation();
                }
            }}
        >
            <Modal
                style={{ minWidth: '40%' }}
                open={!!selectedActionRequest}
                onCancel={onCloseProposalDecisionModal}
                title="Review Proposal"
                footer={
                    <ProposalModalFooter>
                        <ModalButtonContainer>
                            <Button
                                onClick={() => {
                                    onCloseProposalDecisionModal();
                                }}
                                variant="text"
                                color="gray"
                            >
                                Close
                            </Button>
                            <Button
                                data-testid="proposal-accept-button"
                                key="accept"
                                onClick={() => {
                                    onProposalAcceptance();
                                    onCloseProposalDecisionModal();
                                }}
                            >
                                Accept
                            </Button>
                            <Button
                                data-testid="proposal-reject-button"
                                key="reject"
                                onClick={() => {
                                    onProposalRejection();
                                    onCloseProposalDecisionModal();
                                }}
                                color="red"
                                variant="outline"
                            >
                                Reject
                            </Button>
                        </ModalButtonContainer>
                    </ProposalModalFooter>
                }
            >
                <>
                    <ActionRequestListItem actionRequest={actionRequest as ActionRequest} />
                    <StyledForm>
                        <TextArea
                            label=""
                            placeholder="Note - optional"
                            value={note}
                            onChange={(e) => setNote(e.target.value)}
                        />
                    </StyledForm>
                </>
            </Modal>
        </span>
    );
}
