import { Modal, message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import ActionDropdown from '@app/entity/shared/components/styled/search/action/ActionDropdown';
import { SetDomainModal } from '@app/entity/shared/containers/profile/sidebar/Domain/SetDomainModal';
import { handleBatchError } from '@app/entity/shared/utils';

import { useBatchSetDomainMutation } from '@graphql/mutations.generated';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function DomainsDropdown({ urns, disabled = false, refetch }: Props) {
    const { t } = useTranslation('entityV1.shared.components');
    const { t: tc } = useTranslation('common.actions');
    const { t: tl } = useTranslation('common.labels');
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [batchSetDomainMutation] = useBatchSetDomainMutation();

    const batchUnsetDomains = () => {
        batchSetDomainMutation({
            variables: {
                input: {
                    resources: [...urns.map((urn) => ({ resourceUrn: urn }))],
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: t('action.domain.removed'), duration: 2 });
                    refetch?.();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: t('action.domain.removeError', { message: e.message || '' }),
                        duration: 3,
                    }),
                );
            });
    };

    return (
        <>
            <ActionDropdown
                name={tl('domain')}
                actions={[
                    {
                        title: t('action.domain.set'),
                        onClick: () => {
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: t('action.domain.unset'),
                        onClick: () => {
                            Modal.confirm({
                                title: t('action.domain.unsetConfirmTitle'),
                                content: t('action.domain.unsetConfirmContent'),
                                onOk() {
                                    batchUnsetDomains();
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
                <SetDomainModal
                    urns={urns}
                    onCloseModal={() => {
                        setIsEditModalVisible(false);
                        refetch?.();
                    }}
                />
            )}
        </>
    );
}
