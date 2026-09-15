import { Modal } from '@components';
import { message } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import analytics, { EventType } from '@app/analytics';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useUnlinkAssetVersionMutation } from '@graphql/versioning.generated';
import { EntityType } from '@types';

interface Props {
    urn: string;
    entityType: EntityType;
    closeModal: () => void;
    versionSetUrn?: string;
    refetch?: () => void;
}

export default function UnlinkAssetVersionModal({ urn, entityType, closeModal, versionSetUrn, refetch }: Props) {
    const { t } = useTranslation('entity.shared.entityDropdown');
    const { t: tc } = useTranslation('common.actions');
    const entityRegistry = useEntityRegistry();
    const [unlink] = useUnlinkAssetVersionMutation();

    function handleUnlink() {
        unlink({ variables: { input: { unlinkedEntity: urn, versionSet: versionSetUrn } } })
            .then(() => {
                closeModal();
                analytics.event({
                    type: EventType.UnlinkAssetVersionEvent,
                    assetUrn: urn,
                    versionSetUrn,
                    entityType,
                });
                message.loading({
                    content: t('unlinkVersion.loading'),
                    duration: 2,
                });

                setTimeout(() => {
                    refetch?.();
                    message.success({
                        content: t('unlinkVersion.success', {
                            entityName: entityRegistry.getEntityName(entityType),
                        }),
                        duration: 2,
                    });
                }, 2000);
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: t('unlinkVersion.error', { errorMessage: e.message || '' }), duration: 3 });
            });
    }

    return (
        <Modal
            title={t('unlinkVersion.confirmTitle')}
            subtitle={t('unlinkVersion.confirmContent')}
            buttons={[
                { text: tc('no'), variant: 'text', onClick: closeModal, key: 'no' },
                { text: tc('yes'), variant: 'filled', onClick: handleUnlink, key: 'yes' },
            ]}
            onCancel={closeModal}
        />
    );
}
