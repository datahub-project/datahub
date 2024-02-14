import React from 'react';
import styled from 'styled-components';
import { Modal, Button } from 'antd';
import { DataContractProposalParams } from '../../../../../../../../types.generated';
import { FreshnessContractSummary } from '../FreshnessContractSummary';
import { SchemaContractSummary } from '../SchemaContractSummary';
import { DataQualityContractSummary } from '../DataQualityContractSummary';
import { ANTD_GRAY } from '../../../../../constants';

const NoAssertions = styled.div`
    padding: 20px;
    font-size: 16px;
    color: ${ANTD_GRAY[7]};
`;

const ActionButton = styled(Button)``;

type Props = {
    proposal: DataContractProposalParams;
    showActions?: boolean;
    onClose: () => void;
    onApprove?: () => void;
    onDeny?: () => void;
};

/**
 *  Displaying a Data Contract Proposal for an entity.
 */
export const DataContractProposalModal = ({ proposal, showActions = true, onClose, onApprove, onDeny }: Props) => {
    const hasAssertions = proposal.freshness?.length || proposal.schema?.length || proposal.dataQuality?.length;

    return (
        <Modal
            visible
            footer={null}
            onCancel={onClose}
            width={750}
            bodyStyle={{ padding: 0 }}
            title="Review Contract Proposal"
        >
            {!hasAssertions && <NoAssertions>Proposal to remove all assertions from Data Contract</NoAssertions>}
            {(proposal.freshness?.length && <FreshnessContractSummary contracts={proposal.freshness} />) || undefined}
            {(proposal.schema?.length && <SchemaContractSummary contracts={proposal.schema} />) || undefined}
            {(proposal.dataQuality?.length && <DataQualityContractSummary contracts={proposal.dataQuality} />) ||
                undefined}
            {showActions && (
                <>
                    <ActionButton onClick={onApprove}>Approve</ActionButton>
                    <ActionButton onClick={onDeny}>Deny</ActionButton>
                </>
            )}
        </Modal>
    );
};
