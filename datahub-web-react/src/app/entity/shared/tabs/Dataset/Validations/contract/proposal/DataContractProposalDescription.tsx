import React from 'react';
import { Typography } from 'antd';
import { DataContractProposalParams } from '../../../../../../../../types.generated';
import { DBT_URN } from '../../../../../../../ingest/source/builder/constants';

/**
 * Returns true if the contract is implemented externally,
 * e.g. within a source system like Dbt.
 *
 * This differs on a per-system basis.
 */
const isExternalContract = (urn) => {
    if (urn.includes(DBT_URN)) {
        return { isExternal: true, platformName: 'dbt' };
    }
    return { isExternal: false, platformName: undefined };
};

/**
 * Returns the number of assertions that are involved in a particular Data Contract Proposal.
 */
const getAssertionCount = (proposal: DataContractProposalParams) => {
    return (proposal.freshness?.length || 0) + (proposal.dataQuality?.length || 0) + (proposal.schema?.length || 0);
};

type Props = {
    urn: string;
    proposal: DataContractProposalParams;
};

/**
 * A description for a Data Contract Proposal
 */
export const DataContractProposalDescription = ({ urn, proposal }: Props) => {
    const { isExternal, platformName } = isExternalContract(urn);
    const assertionCount = getAssertionCount(proposal);
    return (
        <Typography.Text type="secondary">
            Proposal to create new <b>Data Contract</b> from <b>{assertionCount}</b> assertions.
            {isExternal && (
                <>
                    This contract contains external Assertions which will be provisioned in <b>{platformName}</b>
                </>
            )}
        </Typography.Text>
    );
};
