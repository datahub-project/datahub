import { Button, colors, Text } from '@src/alchemy-components';
import analytics, { EventType, EntityActionType } from '@src/app/analytics';
import { useAcceptProposalsMutation, useRejectProposalsMutation } from '@src/graphql/actionRequest.generated';
import { Divider, message, Modal } from 'antd';
import React from 'react';
import styled from 'styled-components';

const ActionsContainer = styled.div`
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
    bottom: 70px;
    transform: translateX(-48%);
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

interface Props {
    selectedUrns: string[];
    setSelectedUrns: React.Dispatch<React.SetStateAction<string[]>>;
    onActionRequestUpdate: () => void;
}

const ActionsBar = ({ selectedUrns, setSelectedUrns, onActionRequestUpdate }: Props) => {
    const [acceptProposalsMutation] = useAcceptProposalsMutation();
    const [rejectProposalsMutation] = useRejectProposalsMutation();

    const acceptSelectedProposals = () => {
        Modal.confirm({
            title: 'Accept Proposals',
            content: `Are you sure you want to accept these (${selectedUrns.length}) proposals?`,
            okText: 'Yes',
            onOk() {
                acceptProposalsMutation({ variables: { urns: Array.from(selectedUrns) } })
                    .then(() => {
                        analytics.event({
                            type: EventType.BatchEntityActionEvent,
                            actionType: EntityActionType.ProposalsAccepted,
                            entityUrns: Array.from(selectedUrns),
                        });
                        message.success('Accepted proposals!');
                        onActionRequestUpdate();
                        setSelectedUrns([]);
                    })
                    .catch((err) => {
                        console.log(err);
                        message.error('Failed to accept proposals. An unexpected error occurred.');
                    });
            },
        });
    };

    const rejectSelectedProposals = () => {
        Modal.confirm({
            title: 'Reject Proposals',
            content: `Are you sure you want to reject these (${selectedUrns.length}) proposals?`,
            okText: 'Yes',
            onOk() {
                rejectProposalsMutation({ variables: { urns: Array.from(selectedUrns) } })
                    .then(() => {
                        analytics.event({
                            type: EventType.BatchEntityActionEvent,
                            actionType: EntityActionType.ProposalsRejected,
                            entityUrns: Array.from(selectedUrns),
                        });
                        message.success('Proposals declined.');
                        onActionRequestUpdate();
                        setSelectedUrns([]);
                    })
                    .catch((err) => {
                        console.log(err);
                        message.error('Failed to reject proposals. An unexpected error occurred.');
                    });
            },
        });
    };

    return (
        <ActionsContainer>
            <SelectedContainer>
                <Text color="gray">{`${selectedUrns.length} Selected`}</Text>
            </SelectedContainer>
            <VerticalDivider type="vertical" />
            <ButtonsContainer>
                <Button color="red" variant="filled" onClick={rejectSelectedProposals}>
                    Decline All
                </Button>
                <Button color="green" variant="filled" onClick={acceptSelectedProposals}>
                    Approve All
                </Button>
            </ButtonsContainer>
        </ActionsContainer>
    );
};

export default ActionsBar;
