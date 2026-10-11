import { toast } from '@components';
import { Minus } from '@phosphor-icons/react/dist/csr/Minus';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { useEntityData } from '@app/entity/shared/EntityContext';
import {
    ENTITY_HEADER_ACTION_ICON_SIZE,
    ENTITY_HEADER_ACTION_ICON_WEIGHT,
} from '@app/entityV2/shared/EntityDropdown/styledComponents';
import { useIsContractsEnabled } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/actions/useIsContractsEnabled';
import {
    buildAddAssertionToContractMutationVariables,
    buildRemoveAssertionFromContractMutationVariables,
} from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/utils';
import {
    getDataContractCategoryFromAssertion,
    isAssertionPartOfContract,
} from '@app/entityV2/shared/tabs/Dataset/Validations/contract/utils';
import { ActionItem } from '@app/shared/actions/ActionItem';

import { useUpsertDataContractMutation } from '@graphql/contract.generated';
import { Assertion, DataContract } from '@types';

type Props = {
    assertion: Assertion;
    contract?: DataContract | null;
    canEdit: boolean;
    // Should be defined if canEdit
    refetch?: () => void;
    isExpandedView?: boolean;
    onActionTriggered?: () => void;
};

export const ContractAction = ({
    assertion,
    contract,
    canEdit,
    refetch,
    isExpandedView = false,
    onActionTriggered,
}: Props) => {
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
                    toast.success(t('action.addedToContract'), { duration: 2 });
                    refetch?.();
                }
            })
            .catch(() => {
                toast.destroy();
                toast.error(t('action.failedAddToContract'));
            });
    };

    const onRemoveFromContract = () => {
        upsertDataContractMutation({
            variables: buildRemoveAssertionFromContractMutationVariables(entityUrn, assertionUrn, contract),
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(t('action.removedFromContract'), { duration: 2 });
                    refetch?.();
                }
            })
            .catch(() => {
                toast.destroy();
                toast.error(t('action.failedRemoveFromContract'));
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
                    icon={
                        isPartOfContract ? (
                            <Minus size={ENTITY_HEADER_ACTION_ICON_SIZE} weight={ENTITY_HEADER_ACTION_ICON_WEIGHT} />
                        ) : (
                            <Plus size={ENTITY_HEADER_ACTION_ICON_SIZE} weight={ENTITY_HEADER_ACTION_ICON_WEIGHT} />
                        )
                    }
                    isExpandedView={isExpandedView}
                    actionName={contractTip}
                    onActionTriggered={onActionTriggered}
                />
            )) ||
                null}
        </>
    );
};
