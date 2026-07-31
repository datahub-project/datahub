import { Modal, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { UpdateDeprecationModal } from '@app/entity/shared/EntityDropdown/UpdateDeprecationModal';
import ActionDropdown from '@app/entity/shared/components/styled/search/action/ActionDropdown';
import { handleBatchError } from '@app/entity/shared/utils';

import { useBatchUpdateDeprecationMutation } from '@graphql/mutations.generated';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function DeprecationDropdown({ urns, disabled = false, refetch }: Props) {
    const { t } = useTranslation('entityV1.shared.components');
    const { t: tc } = useTranslation('common.actions');
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
                    message.success({ content: t('action.deprecation.undeprecatedSuccess'), duration: 2 });
                    refetch?.();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: t('action.deprecation.undeprecatedError', { message: e.message || '' }),
                        duration: 3,
                    }),
                );
            });
    };

    return (
        <>
            <ActionDropdown
                name={t('action.deprecation.name')}
                actions={[
                    {
                        title: t('action.deprecation.markDeprecated'),
                        onClick: () => {
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: t('action.deprecation.markUndeprecated'),
                        onClick: () => {
                            Modal.confirm({
                                title: t('action.deprecation.confirmTitle'),
                                content: t('action.deprecation.confirmContent'),
                                onOk() {
                                    batchUndeprecate();
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
