import { message, Modal } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useBatchUpdateDeprecationMutation } from '../../../../../../../graphql/mutations.generated';
import { UpdateDeprecationModal } from '../../../../EntityDropdown/UpdateDeprecationModal';
import ActionDropdown from './ActionDropdown';
import { handleBatchError } from '../../../../utils';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function DeprecationDropdown({ urns, disabled = false, refetch }: Props) {
    const { t } = useTranslation();
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
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
                    message.success({ content: t('deprecation.markAssetsAsUnDeprecatedSuccess'), duration: 2 });
                    refetch?.();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: `${t('deprecation.markAssetsAsUnDeprecatedError')} \n ${e.message || ''}`,
                        duration: 3,
                    }),
                );
            });
    };

    return (
        <>
            <ActionDropdown
                name={t('common.deprecation')}
                actions={[
                    {
                        title: t('deprecation.markAsDeprecated'),
                        onClick: () => {
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: t('deprecation.markAsUnDeprecated'),
                        onClick: () => {
                            Modal.confirm({
                                title: t('deprecation.markAssetsAsUnDeprecatedModalTitle'),
                                content: t('deprecation.markAssetsAsUnDeprecatedModalContent'),
                                onOk() {
                                    batchUndeprecate();
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
            {isEditModalVisible && (
                <UpdateDeprecationModal
                    urns={urns}
                    onClose={() => {
                        setIsEditModalVisible(false);
                        refetch?.();
                    }}
                />
            )}
        </>
    );
}
