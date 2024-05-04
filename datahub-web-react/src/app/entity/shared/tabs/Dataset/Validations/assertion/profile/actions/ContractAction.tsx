import React from 'react';

import styled from 'styled-components';
import { MinusOutlined, PlusOutlined } from '@ant-design/icons';
import { message } from 'antd';

import { ActionItem } from './ActionItem';
import { Assertion, DataContract, Monitor } from '../../../../../../../../../types.generated';
import { useUpsertDataContractMutation } from '../../../../../../../../../graphql/contract.generated';
import {
    buildAddAssertionToContractMutationVariables,
    buildRemoveAssertionFromContractMutationVariables,
} from '../../../contract/builder/utils';
import { getDataContractCategoryFromAssertion, isAssertionPartOfContract } from '../../../contract/utils';
import { useEntityData } from '../../../../../../EntityContext';
import { useIsContractsEnabled } from './useIsContractsEnabled';

const StyledMinusOutlined = styled(MinusOutlined)`
    && {
        font-size: 12px;
        display: flex;
    }
`;

const StyledPlusOutlined = styled(PlusOutlined)`
    && {
        font-size: 12px;
        display: flex;
    }
`;

type Props = {
    assertion: Assertion;
    monitor?: Monitor;
    contract?: DataContract;
    canEdit: boolean;
    // Should be defined if canEdit
    refetch?: () => void;
};

export const ContractAction = ({ assertion, monitor, contract, canEdit, refetch }: Props) => {
    const { urn: entityUrn } = useEntityData();
    const [upsertDataContractMutation] = useUpsertDataContractMutation();
    const contractsEnabled = useIsContractsEnabled();

    if (!monitor || !entityUrn || !contractsEnabled) {
        return null;
    }

    const assertionUrn = assertion.urn;

    const onAddToContract = () => {
        const category = getDataContractCategoryFromAssertion(assertion);
        upsertDataContractMutation({
            variables: buildAddAssertionToContractMutationVariables(category, entityUrn, assertionUrn, contract),
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: 'Added assertion to contract!', duration: 2 });
                    refetch?.();
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to add Assertion to Contract. An unexpected error occurred' });
            });
    };

    const onRemoveFromContract = () => {
        upsertDataContractMutation({
            variables: buildRemoveAssertionFromContractMutationVariables(entityUrn, assertionUrn, contract),
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: 'Removed assertion from contract.', duration: 2 });
                    refetch?.();
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to remove Assertion from Contract. An unexpected error occurred' });
            });
    };

    const isPartOfContract = contract ? isAssertionPartOfContract(assertion, contract) : false;
    const isPartOfContractTip = isPartOfContract ? 'Remove from contract' : 'Add to contract';

    const unauthorizedTip = canEdit ? undefined : 'You do not have permission to edit the contract';
    const tip = canEdit ? isPartOfContractTip : unauthorizedTip;

    return (
        <>
            {(entityUrn && (
                <ActionItem
                    key="0"
                    tip={tip}
                    disabled={!canEdit}
                    onClick={isPartOfContract ? onRemoveFromContract : onAddToContract}
                    icon={isPartOfContract ? <StyledMinusOutlined /> : <StyledPlusOutlined />}
                />
            )) ||
                null}
        </>
    );
};
