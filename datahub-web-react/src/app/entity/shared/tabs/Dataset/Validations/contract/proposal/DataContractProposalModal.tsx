import { Button, Modal } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { DataQualityContractSummary } from '@app/entity/shared/tabs/Dataset/Validations/contract/DataQualityContractSummary';
import { FreshnessContractSummary } from '@app/entity/shared/tabs/Dataset/Validations/contract/FreshnessContractSummary';
import { SchemaContractSummary } from '@app/entity/shared/tabs/Dataset/Validations/contract/SchemaContractSummary';

import { DataContractProposalParams } from '@types';

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
