import { toast } from '@components';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import {
    ENTITY_HEADER_ACTION_ICON_SIZE,
    ENTITY_HEADER_ACTION_ICON_WEIGHT,
} from '@app/entityV2/shared/EntityDropdown/styledComponents';
import { useDeleteAssertionMutationWithCache } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/hooks';
import { ActionItem } from '@app/shared/actions';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { Assertion } from '@types';

type Props = {
    assertion: Assertion;
    canEdit: boolean;
    refetch?: () => void;
    isExpandedView?: boolean;
    onActionTriggered?: () => void;
};

export const DeleteAction = ({ assertion, canEdit, refetch, isExpandedView = false, onActionTriggered }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const { t: tc } = useTranslation('common.actions');
    const [deleteAssertionMutation] = useDeleteAssertionMutationWithCache();
    const [showConfirmationModal, setShowConfirmationModal] = useState(false);

    const deleteAssertion = async () => {
        setShowConfirmationModal(false);
        try {
            const response = await deleteAssertionMutation({ variables: { urn: assertion.urn } });
            if (!response.data?.deleteAssertion) {
                throw new Error('Assertion deletion was not acknowledged');
            }
            toast.success(t('action.removedAssertion'), { duration: 2 });
            refetch?.();
        } catch {
            toast.destroy();
            toast.error(t('action.failedRemoveAssertion'), { duration: 3 });
        }
    };

    return (
        <>
            <ActionItem
                key="delete"
                tip={canEdit ? t('action.deleteAssertionTip') : t('action.noPermissionDelete')}
                disabled={!canEdit}
                onClick={() => setShowConfirmationModal(true)}
                icon={<Trash size={ENTITY_HEADER_ACTION_ICON_SIZE} weight={ENTITY_HEADER_ACTION_ICON_WEIGHT} />}
                isExpandedView={isExpandedView}
                actionName={tc('delete')}
                onActionTriggered={onActionTriggered}
            />
            <ConfirmationModal
                isOpen={showConfirmationModal}
                handleClose={() => setShowConfirmationModal(false)}
                handleConfirm={deleteAssertion}
                modalTitle={t('action.confirmRemovalTitle')}
                modalText={t('action.confirmRemovalContent')}
                confirmButtonText={tc('yes')}
                isDeleteModal
            />
        </>
    );
};
