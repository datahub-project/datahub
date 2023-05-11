import { message, Modal } from 'antd';
import React, { useState } from 'react';
import ActionDropdown from './ActionDropdown';
import { handleBatchError } from '../../../../utils';
import { useBatchSetDataProductMutation } from '../../../../../../../graphql/dataProduct.generated';
import SetDataProductModal from '../../../../containers/profile/sidebar/DataProduct/SetDataProductModal';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function DataProductsDropdown({ urns, disabled = false, refetch }: Props) {
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
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
                            Modal.confirm({
                                title: `If you continue, Data Product will be removed for the selected assets.`,
                                content: `Are you sure you want to unset Data Product for these assets?`,
                                onOk() {
                                    batchUnsetDataProducts();
                                },
                                onCancel() {},
                                okText: 'Yes',
                                maskClosable: true,
                                closable: true,
                            });
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
        </>
    );
}
