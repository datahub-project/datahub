import analytics, { EventType } from '@app/analytics';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Modal } from '@components';
import { useUnlinkAssetVersionMutation } from '@graphql/versioning.generated';
import { EntityType } from '@types';
import { message } from 'antd';
import React from 'react';

interface Props {
    urn: string;
    entityType: EntityType;
    closeModal: () => void;
    versionSetUrn?: string;
    refetch?: () => void;
}

export default function UnlinkAssetVersionModal({ urn, entityType, closeModal, versionSetUrn, refetch }: Props) {
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
                    content: 'Unlinking...',
                    duration: 2,
                });

                setTimeout(() => {
                    refetch?.();
                    message.success({
                        content: `Unlinked ${entityRegistry.getEntityName(entityType)}!`,
                        duration: 2,
                    });
                }, 2000);
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to unlink: \n ${e.message || ''}`, duration: 3 });
            });
    }

    return (
        <Modal
            title="Are you sure?"
            subtitle="Would you like to unlink this version?"
            buttons={[
                { text: 'No', variant: 'text', onClick: closeModal },
                { text: 'Yes', variant: 'filled', onClick: handleUnlink },
            ]}
            onCancel={closeModal}
        />
    );
}
