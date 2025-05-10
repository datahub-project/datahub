import { useApolloClient } from '@apollo/client';
import { Divider, Form, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { updateActionRequestsList } from '@app/taskCenterV2/proposalsV2/proposalsTable/cacheUtils';
import { ProposalModalType } from '@app/taskCenterV2/proposalsV2/utils';
import { Button, Modal, Text, TextArea, colors } from '@src/alchemy-components';
import analytics, { EntityActionType, EventType } from '@src/app/analytics';
import { useAcceptProposalsMutation, useRejectProposalsMutation } from '@src/graphql/actionRequest.generated';

import { ActionRequestResult } from '@types';

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
`;

const VerticalDivider = styled(Divider)`
    height: auto;
    margin: 0 4px;
    align-self: stretch;
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
    onActionRequestUpdate: (completedUrns: string[]) => void;
    hasPagination?: boolean;
}

const ActionsBar = ({ selectedUrns, setSelectedUrns, onActionRequestUpdate, hasPagination }: Props) => {
    const [modalType, setModalType] = useState<ModalType>(null);
    const [note, setNote] = useState('');

    const [acceptProposalsMutation] = useAcceptProposalsMutation();
    const [rejectProposalsMutation] = useRejectProposalsMutation();

    const client = useApolloClient();

    const authenticatedUser = useUserContext();
    const currentUser = authenticatedUser?.user;

    const acceptSelectedProposals = () => {
        acceptProposalsMutation({
            variables: { urns: selectedUrns, note },
        })
            .then(() => {
                analytics.event({
                    type: EventType.BatchEntityActionEvent,
                    actionType: EntityActionType.ProposalsAccepted,
                    entityUrns: selectedUrns,
                });
                message.success('Accepted proposals!');
                updateActionRequestsList(client, selectedUrns, ActionRequestResult.Accepted, note, currentUser);
                onActionRequestUpdate(selectedUrns);
                setSelectedUrns([]);
            })
            .catch((err) => {
                console.log(err);
                message.error('Failed to accept proposals. An unexpected error occurred.');
            });
    };

    const rejectSelectedProposals = () => {
        rejectProposalsMutation({
            variables: { urns: selectedUrns, note },
        })
            .then(() => {
                analytics.event({
                    type: EventType.BatchEntityActionEvent,
                    actionType: EntityActionType.ProposalsRejected,
                    entityUrns: selectedUrns,
                });
                message.success('Proposals declined.');
                updateActionRequestsList(client, selectedUrns, ActionRequestResult.Rejected, note, currentUser);
                onActionRequestUpdate(selectedUrns);
                setSelectedUrns([]);
            })
            .catch((err) => {
                console.log(err);
                message.error('Failed to reject proposals. An unexpected error occurred.');
            });
    };

    const handleCancel = () => {
        setModalType(null);
        setNote('');
    };

    return (
        <ActionsContainer $hasPagination={hasPagination}>
            <SelectedContainer>
                <Text color="gray">{`${selectedUrns.length} Selected`}</Text>
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
                            ? `Are you sure you want to accept these (${selectedUrns.length}) proposals?`
                            : `Are you sure you want to reject these (${selectedUrns.length}) proposals?`
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
