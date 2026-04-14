import { message } from 'antd';
import React, { useState } from 'react';

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
                    message.success({ content: 'Deleted assets!', duration: 2 });
                    setTimeout(() => refetch?.(), 3000);
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: `Failed to delete assets: \n ${e.message || ''}`,
                        duration: 3,
                    }),
                );
            });
    };

    return (
        <>
            <ActionDropdown
                name="Delete"
                actions={[
                    {
                        title: 'Mark as deleted',
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
                modalTitle="Confirm Delete"
                modalText="Are you sure you want to mark these assets as deleted? This will hide the assets
                                from future DataHub searches. If the assets are re-ingested from an external data platform, they will be restored."
            />
        </>
    );
}
