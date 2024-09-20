import React from 'react';

import { Button, Modal } from 'antd';
import styled from 'styled-components';
import ActionRequestListItem from '@src/app/actionrequest/item/ActionRequestListItem';
import { ActionRequest } from '../../../types.generated';

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
                    <div>
                        <Button
                            onClick={(e) => {
                                onCloseProposalDecisionModal(e);
                            }}
                            type="text"
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
                            type="primary"
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
                        >
                            Reject
                        </Button>
                    </div>
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
