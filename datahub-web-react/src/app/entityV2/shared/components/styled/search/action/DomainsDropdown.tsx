import { message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import ActionDropdown from '@app/entityV2/shared/components/styled/search/action/ActionDropdown';
import { SetDomainModal } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SetDomainModal';
import { handleBatchError } from '@app/entityV2/shared/utils';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { useBatchSetDomainMutation } from '@graphql/mutations.generated';

type Props = {
    urns: Array<string>;
    disabled: boolean;
    refetch?: () => void;
};

// eslint-disable-next-line
export default function DomainsDropdown({ urns, disabled = false, refetch }: Props) {
    const { t } = useTranslation('entity.shared.components');
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);
    const [isUnsetModalVisible, setIsUnsetModalVisible] = useState(false);

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
                    message.success({ content: t('searchActions.domain.removedSuccess'), duration: 2 });
                    refetch?.();
                }
                setIsUnsetModalVisible(false);
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: t('searchActions.domain.removeError', { message: e.message || '' }),
                        duration: 3,
                    }),
                );
            });
    };

    return (
        <>
            <ActionDropdown
                name={t('searchActions.domain.name')}
                actions={[
                    {
                        title: t('searchActions.domain.setTitle'),
                        onClick: () => {
                            setIsEditModalVisible(true);
                        },
                    },
                    {
                        title: t('searchActions.domain.unsetTitle'),
                        onClick: () => {
                            setIsUnsetModalVisible(true);
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
            <ConfirmationModal
                isOpen={isUnsetModalVisible}
                handleClose={() => setIsUnsetModalVisible(false)}
                handleConfirm={batchUnsetDomains}
                modalTitle={t('searchActions.domain.removeConfirmTitle')}
                modalText={t('searchActions.domain.removeConfirmText')}
            />
        </>
    );
}
