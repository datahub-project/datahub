import { Modal, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import ActionDropdown from '@app/entity/shared/components/styled/search/action/ActionDropdown';
import SetDataProductModal from '@app/entity/shared/containers/profile/sidebar/DataProduct/SetDataProductModal';
import { handleBatchError } from '@app/entity/shared/utils';

import { useBatchSetDataProductMutation } from '@graphql/dataProduct.generated';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function DataProductsDropdown({ urns, disabled = false, refetch }: Props) {
    const { t } = useTranslation('entityV1.shared.components');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
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
                    message.loading({ content: tf('loading'), duration: 2 });
                    setTimeout(() => {
                        message.success({ content: t('action.dataProduct.removed'), duration: 2 });
                        refetch?.();
                    }, 2000);
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: t('action.dataProduct.removeError', { message: e.message || '' }),
                        duration: 3,
                    }),
                );
            });
    };

    return (
        <>
            <ActionDropdown
                name={t('action.dataProduct.name')}
                actions={[
                    {
                        title: t('action.dataProduct.set'),
                        onClick: () => {
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: t('action.dataProduct.unset'),
                        onClick: () => {
                            Modal.confirm({
                                title: t('action.dataProduct.unsetConfirmTitle'),
                                content: t('action.dataProduct.unsetConfirmContent'),
                                onOk() {
                                    batchUnsetDataProducts();
                                },
                                onCancel() {},
                                okText: tc('yes'),
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
