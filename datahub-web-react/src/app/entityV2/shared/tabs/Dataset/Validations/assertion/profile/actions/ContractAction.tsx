import React from 'react';

import styled from 'styled-components';
import { MinusOutlined, PlusOutlined } from '@ant-design/icons';
import { message } from 'antd';

import { ActionItem } from './ActionItem';
import { Assertion, DataContract } from '../../../../../../../../../types.generated';
import { useUpsertDataContractMutation } from '../../../../../../../../../graphql/contract.generated';
import {
    buildAddAssertionToContractMutationVariables,
    buildRemoveAssertionFromContractMutationVariables,
} from '../../../contract/builder/utils';
import { getDataContractCategoryFromAssertion, isAssertionPartOfContract } from '../../../contract/utils';
import { useEntityData } from '../../../../../../../../entity/shared/EntityContext';
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
    contract?: DataContract;
    canEdit: boolean;
    // Should be defined if canEdit
    refetch?: () => void;
    isExpandedView?: boolean;
};

export const ContractAction = ({ assertion, contract, canEdit, refetch, isExpandedView = false }: Props) => {
    const { urn: entityUrn } = useEntityData();
    const [upsertDataContractMutation] = useUpsertDataContractMutation();
    const contractsEnabled = useIsContractsEnabled();

    if (!entityUrn || !contractsEnabled) {
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
    const contractTip = isPartOfContract ? 'Remove from contract' : 'Add to contract';

    const unauthorizedTip = canEdit ? undefined : 'You do not have permission to edit the contract';
    const tip = canEdit ? contractTip : unauthorizedTip;

    return (
        <>
            {(entityUrn && (
                <ActionItem
                    key="0"
                    tip={tip}
                    disabled={!canEdit}
                    onClick={isPartOfContract ? onRemoveFromContract : onAddToContract}
                    icon={isPartOfContract ? <StyledMinusOutlined /> : <StyledPlusOutlined />}
                    isExpandedView={isExpandedView}
                    actionName={contractTip}
                />
            )) ||
                null}
        </>
    );
};
