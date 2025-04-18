import { Modal } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ModalButtonContainer } from '@app/shared/button/styledComponents';
import { Button } from '@src/alchemy-components';
import ActionRequestListItem from '@src/app/actionrequest/item/ActionRequestListItem';

import { ActionRequest } from '@types';

type ProposalModalProps = {
    actionRequest: ActionRequest;
    showProposalDecisionModal: boolean;
    onCloseProposalDecisionModal: (e) => void;
    onProposalAcceptance: (ActionRequest) => void;
    onProposalRejection: (ActionRequest) => void;
    onActionRequestUpdate: () => void;
    elementName: any;
};

const ProposalModalFooter = styled.div`
    display: flex;
    justify-content: end;
`;

export default function ProposalModal({
    actionRequest,
    showProposalDecisionModal,
    onCloseProposalDecisionModal,
    onProposalAcceptance,
    onProposalRejection,
    onActionRequestUpdate,
    elementName,
}: ProposalModalProps) {
    return (
        <Modal
            style={{ minWidth: '40%' }}
            visible={showProposalDecisionModal}
            onCancel={(e) => {
                onCloseProposalDecisionModal(e);
            }}
            title="Review Proposal"
            footer={
                <ProposalModalFooter>
                    <ModalButtonContainer>
                        <Button
                            onClick={(e) => {
                                onCloseProposalDecisionModal(e);
                            }}
                            variant="text"
                            color="gray"
                        >
                            Close
                        </Button>
                        <Button
                            data-testid={`proposal-accept-button-${elementName}`}
                            key="accept"
                            onClick={(e) => {
                                onProposalAcceptance(actionRequest);
                                onCloseProposalDecisionModal(e);
                            }}
                        >
                            Accept
                        </Button>
                        <Button
                            data-testid={`proposal-reject-button-${elementName}`}
                            key="reject"
                            onClick={(e) => {
                                onProposalRejection(actionRequest);
                                onCloseProposalDecisionModal(e);
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
            <ActionRequestListItem
                actionRequest={actionRequest as ActionRequest}
                onUpdate={onActionRequestUpdate}
                showActionsButtons={false}
            />
        </Modal>
    );
}
