import { MinusOutlined, PlusOutlined } from '@ant-design/icons';
import { message } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { ActionItem } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/ActionItem';
import { useIsContractsEnabled } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/useIsContractsEnabled';
import {
    buildAddAssertionToContractMutationVariables,
    buildRemoveAssertionFromContractMutationVariables,
} from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/utils';
import {
    getDataContractCategoryFromAssertion,
    isAssertionPartOfContract,
} from '@app/entityV2/shared/tabs/Dataset/Validations/contract/utils';

import { useUpsertDataContractMutation } from '@graphql/contract.generated';
import { Assertion, DataContract } from '@types';

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
    const { t } = useTranslation('entity.profile.validations');
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
                    message.success({ content: t('action.addedToContract'), duration: 2 });
                    refetch?.();
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: t('action.failedAddToContract') });
            });
    };

    const onRemoveFromContract = () => {
        upsertDataContractMutation({
            variables: buildRemoveAssertionFromContractMutationVariables(entityUrn, assertionUrn, contract),
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: t('action.removedFromContract'), duration: 2 });
                    refetch?.();
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: t('action.failedRemoveFromContract') });
            });
    };

    const isPartOfContract = contract ? isAssertionPartOfContract(assertion, contract) : false;
    const contractTip = isPartOfContract ? t('action.removeFromContract') : t('action.addToContract');

    const unauthorizedTip = canEdit ? undefined : t('action.noPermissionEditContract');
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
