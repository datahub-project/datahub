import { message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import ActionDropdown from '@app/entityV2/shared/components/styled/search/action/ActionDropdown';
import { handleBatchError } from '@app/entityV2/shared/utils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { useBatchUpdateSoftDeletedMutation } from '@graphql/mutations.generated';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function DeleteDropdown({ urns, disabled = false, refetch }: Props) {
    const { t } = useTranslation('entity.shared.components');
    const { t: tc } = useTranslation('common.actions');
    const [batchUpdateSoftDeletedMutation] = useBatchUpdateSoftDeletedMutation();
    const [showConfirmModal, setShowConfirmModal] = useState(false);

    const batchSoftDelete = () => {
        batchUpdateSoftDeletedMutation({
            variables: {
                input: {
                    urns,
                    deleted: true,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: t('searchActions.delete.success'), duration: 2 });
                    setTimeout(() => refetch?.(), 3000);
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: t('searchActions.delete.error', { message: e.message || '' }),
                        duration: 3,
                    }),
                );
            });
    };

    return (
        <>
            <ActionDropdown
                name={tc('delete')}
                actions={[
                    {
                        title: t('searchActions.delete.markTitle'),
                        onClick: () => {
                            setShowConfirmModal(true);
                        },
                    },
                ]}
                disabled={disabled}
            />
            <ConfirmationModal
                isOpen={showConfirmModal}
                handleClose={() => setShowConfirmModal(false)}
                handleConfirm={batchSoftDelete}
                modalTitle={t('searchActions.delete.confirmTitle')}
                modalText={t('searchActions.delete.confirmText')}
            />
        </>
    );
}
