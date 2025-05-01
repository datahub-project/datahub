import { message } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useRefetch } from '@app/entity/shared/EntityContext';
import { ModalButtonContainer } from '@app/shared/button/styledComponents';
import { Button, Modal } from '@src/alchemy-components';
import { ActionRequestListItem } from '@src/app/actionrequestV2/item/ActionRequestListItem';

import { useAcceptProposalsMutation, useRejectProposalsMutation } from '@graphql/actionRequest.generated';
import { ActionRequest } from '@types';

const ProposalModalFooter = styled.div`
    display: flex;
    justify-content: end;
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

    const handleRefetch = () => {
        if (refetch) refetch();
        entityRefetch();
    };

    const onCloseProposalDecisionModal = () => {
        setSelectedActionRequest(null);
        setTimeout(() => handleRefetch(), 2000);
    };

    const onProposalAcceptance = () => {
        acceptProposalsMutation({ variables: { urns: [actionRequest.urn] } })
            .then(() => {
                message.success('Successfully accepted the proposal!');
            })
            .then(handleRefetch)
            .catch((err) => {
                console.log(err);
                message.error('Failed to accept proposal. :(');
            });
    };

    const onProposalRejection = () => {
        rejectProposalsMutation({ variables: { urns: [actionRequest.urn] } })
            .then(() => {
                message.info('Proposal declined.');
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
                <ActionRequestListItem actionRequest={actionRequest as ActionRequest} />
            </Modal>
        </span>
    );
}
