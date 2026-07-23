import { message } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import analytics, { EventType } from '@app/analytics';
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
    const { t } = useTranslation('entity.shared.components');
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
                    message.success({ content: t('deprecation.markedUnDeprecatedSuccess'), duration: 2 });
                    refetch?.();
                    analytics.event({
                        type: EventType.SetDeprecation,
                        entityUrns: urns,
                        deprecated: false,
                    });
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urns, e, {
                        content: t('deprecation.markUnDeprecatedError', { message: e.message || '' }),
                        duration: 3,
                    }),
                );
            });
    };

    return (
        <>
            <ActionDropdown
                name={t('searchActions.deprecation.name')}
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
                modalTitle={t('deprecation.confirmUnDeprecatedTitle')}
                modalText={t('deprecation.confirmUnDeprecatedText')}
            />
        </>
    );
}
