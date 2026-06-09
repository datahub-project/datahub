import { message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import ActionDropdown from '@app/entityV2/shared/components/styled/search/action/ActionDropdown';
import SetDataProductModal from '@app/entityV2/shared/containers/profile/sidebar/DataProduct/SetDataProductModal';
import { handleBatchError } from '@app/entityV2/shared/utils';
import { useIsMultipleDataProductsEnabled } from '@app/shared/hooks/useIsMultipleDataProductsEnabled';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { useBatchSetDataProductMutation } from '@graphql/dataProduct.generated';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function DataProductsDropdown({ urns, disabled = false, refetch }: Props) {
    const { t } = useTranslation('entity.shared.components');
    const { t: tcf } = useTranslation('common.feedback');
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [isUnsetModalVisible, setIsUnsetModalVisible] = useState(false);
    const isMultipleDataProductsEnabled = useIsMultipleDataProductsEnabled();
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
                    message.loading({ content: tcf('loading'), duration: 2 });
                    setTimeout(() => {
                        message.success({ content: t('searchActions.dataProduct.removedSuccess'), duration: 2 });
                        refetch?.();
                    }, 2000);
                }
                setIsUnsetModalVisible(false);
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: t('searchActions.dataProduct.removeError', { message: e.message || '' }),
                        duration: 3,
                    }),
                );
            });
    };

    return (
        <>
            <ActionDropdown
                name={
                    isMultipleDataProductsEnabled
                        ? t('searchActions.dataProduct.nameMultiple')
                        : t('searchActions.dataProduct.nameSingle')
                }
                actions={[
                    {
                        title: isMultipleDataProductsEnabled
                            ? t('searchActions.dataProduct.addMultiple')
                            : t('searchActions.dataProduct.setSingle'),
                        onClick: () => {
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: isMultipleDataProductsEnabled
                            ? t('searchActions.dataProduct.unsetMultiple')
                            : t('searchActions.dataProduct.unsetSingle'),
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
                    currentDataProducts={[]}
                    onModalClose={() => {
                        setIsEditModalVisible(false);
                    }}
                />
            )}
            <ConfirmationModal
                isOpen={isUnsetModalVisible}
                handleClose={() => setIsUnsetModalVisible(false)}
                handleConfirm={batchUnsetDataProducts}
                modalTitle={
                    isMultipleDataProductsEnabled
                        ? t('searchActions.dataProduct.removeConfirmTitleMultiple')
                        : t('searchActions.dataProduct.removeConfirmTitleSingle')
                }
                modalText={
                    isMultipleDataProductsEnabled
                        ? t('searchActions.dataProduct.removeConfirmTextMultiple')
                        : t('searchActions.dataProduct.removeConfirmTextSingle')
                }
            />
        </>
    );
}
