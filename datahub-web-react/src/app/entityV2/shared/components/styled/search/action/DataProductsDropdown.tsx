import { message } from 'antd';
import React, { useState } from 'react';

import ActionDropdown from '@app/entityV2/shared/components/styled/search/action/ActionDropdown';
import SetDataProductModal from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/SetDataProductModal';
import { handleBatchError } from '@app/entityV2/shared/utils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { useBatchSetDataProductMutation } from '@graphql/dataProduct.generated';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function DataProductsDropdown({ urns, disabled = false, refetch }: Props) {
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [isUnsetModalVisible, setIsUnsetModalVisible] = useState(false);
    const [batchSetDataProductMutation] = useBatchSetDataProductMutation();

    const batchUnsetDataProducts = () => {
        batchSetDataProductMutation({
            variables: {
                input: {
                    resourceUrns: urns,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.loading({ content: 'Loading...', duration: 2 });
                    setTimeout(() => {
                        message.success({ content: 'Removed Data Product!', duration: 2 });
                        refetch?.();
                    }, 2000);
                }
                setIsUnsetModalVisible(false);
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: `Failed to remove assets from Data Product: \n ${e.message || ''}`,
                        duration: 3,
                    }),
                );
            });
    };

    return (
        <>
            <ActionDropdown
                name="Data Product"
                actions={[
                    {
                        title: 'Set Data Product',
                        onClick: () => {
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: 'Unset Data Product',
                        onClick: () => {
                            setIsUnsetModalVisible(true);
                        },
                    },
                ]}
                disabled={disabled}
            />
            {isEditModalVisible && (
                <SetDataProductModal
                    urns={urns}
                    currentDataProduct={null}
                    onModalClose={() => {
                        setIsEditModalVisible(false);
                    }}
                    refetch={refetch}
                />
            )}
            <ConfirmationModal
                isOpen={isUnsetModalVisible}
                handleClose={() => setIsUnsetModalVisible(false)}
                handleConfirm={batchUnsetDataProducts}
                modalTitle="Data Product will be removed for the selected assets"
                modalText="Are you sure you want to unset Data Product for these assets?"
            />
        </>
    );
}
