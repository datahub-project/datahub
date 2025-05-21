import { message } from 'antd';
import React, { useState } from 'react';

import { UpdateDeprecationModal } from '@app/entityV2/shared/EntityDropdown/UpdateDeprecationModal';
import ActionDropdown from '@app/entityV2/shared/components/styled/search/action/ActionDropdown';
import { handleBatchError } from '@app/entityV2/shared/utils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { useBatchUpdateDeprecationMutation } from '@graphql/mutations.generated';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function DeprecationDropdown({ urns, disabled = false, refetch }: Props) {
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [showConfirmationModal, setShowConfirmationModal] = useState(false);

    const [batchUpdateDeprecationMutation] = useBatchUpdateDeprecationMutation();

    const batchUndeprecate = () => {
        batchUpdateDeprecationMutation({
            variables: {
                input: {
                    resources: [...urns.map((urn) => ({ resourceUrn: urn }))],
                    deprecated: false,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: 'Marked assets as un-deprecated!', duration: 2 });
                    refetch?.();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: `Failed to mark assets as un-deprecated: \n ${e.message || ''}`,
                        duration: 3,
                    }),
                );
            });
    };

    return (
        <>
            <ActionDropdown
                name="Deprecation"
                actions={[
                    {
                        title: 'Mark as deprecated',
                        onClick: () => {
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: 'Mark as un-deprecated',
                        onClick: () => {
                            setShowConfirmationModal(true);
                        },
                    },
                ]}
                disabled={disabled}
            />
            {isEditModalVisible && (
                <UpdateDeprecationModal
                    urns={urns}
                    onClose={() => {
                        setIsEditModalVisible(false);
                        refetch?.();
                    }}
                />
            )}
            <ConfirmationModal
                isOpen={showConfirmationModal}
                handleClose={() => setShowConfirmationModal(false)}
                handleConfirm={batchUndeprecate}
                modalTitle="Confirm Mark as un-deprecated"
                modalText="Are you sure you want to mark these assets as un-deprecated?"
            />
        </>
    );
}
