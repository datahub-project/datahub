import { message, Modal } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useBatchUpdateSoftDeletedMutation } from '../../../../../../../graphql/mutations.generated';
import ActionDropdown from './ActionDropdown';
import { handleBatchError } from '../../../../utils';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function DeleteDropdown({ urns, disabled = false, refetch }: Props) {
    const { t } = useTranslation();
    const [batchUpdateSoftDeletedMutation] = useBatchUpdateSoftDeletedMutation();

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
                    message.success({ content: t('common.deletedAssets'), duration: 2 });
                    setTimeout(() => refetch?.(), 3000);
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: `${t('crud.error.delete')} \n ${e.message || ''}`,
                        duration: 3,
                    }),
                );
            });
    };

    return (
        <>
            <ActionDropdown
                name={t('crud.delete')}
                actions={[
                    {
                        title: t('crud.markAsDeleted'),
                        onClick: () => {
                            Modal.confirm({
                                title: t('crud.doYouWantTo.confirmDelete'),
                                content: t('entity.deleteAssetMessageConfirmation'),
                                onOk() {
                                    batchSoftDelete();
                                },
                                onCancel() {},
                                okText: t('common.yes'),
                                maskClosable: true,
                                closable: true,
                            });
                        },
                    },
                ]}
                disabled={disabled}
            />
        </>
    );
}
