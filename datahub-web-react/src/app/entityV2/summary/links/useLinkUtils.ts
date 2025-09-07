import { message } from 'antd';
import { useEffect, useState } from 'react';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';

import { useAddLinkMutation, useRemoveLinkMutation, useUpdateLinkMutation } from '@graphql/mutations.generated';
import { InstitutionalMemoryMetadata } from '@types';

export function useLinkUtils(selectedLink: InstitutionalMemoryMetadata | null = null) {
    const { urn: entityUrn, entityType } = useEntityData();
    const refetch = useRefetch();
    const mutationUrn = useMutationUrn();
    const [showInAssetPreview, setShowInAssetPreview] = useState(!!selectedLink?.settings?.showInAssetPreview);

    const [removeLinkMutation] = useRemoveLinkMutation();
    const [addLinkMutation] = useAddLinkMutation();
    const [updateLinkMutation] = useUpdateLinkMutation();

    useEffect(() => {
        if (selectedLink) {
            setShowInAssetPreview(!!selectedLink?.settings?.showInAssetPreview);
        }
    }, [selectedLink, selectedLink?.settings?.showInAssetPreview]);

    const handleDeleteLink = async () => {
        if (!selectedLink) {
            return;
        }
        try {
            await removeLinkMutation({
                variables: {
                    input: {
                        linkUrl: selectedLink.url,
                        label: selectedLink.label || selectedLink.description,
                        resourceUrn: selectedLink.associatedUrn || entityUrn,
                    },
                },
            });
            message.success({ content: 'Link Removed', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Error removing link: \n ${e.message || ''}`, duration: 2 });
            }
        }
        refetch?.();
    };

    const handleAddLink = async (formValues) => {
        try {
            await addLinkMutation({
                variables: {
                    input: {
                        linkUrl: formValues.url,
                        label: formValues.label,
                        resourceUrn: mutationUrn,
                        settings: { showInAssetPreview },
                    },
                },
            });
            message.success({ content: 'Link Added', duration: 2 });
            analytics.event({
                type: EventType.EntityActionEvent,
                entityType,
                entityUrn: mutationUrn,
                actionType: EntityActionType.UpdateLinks,
            });
            refetch?.();
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to add link: \n ${e.message || ''}`, duration: 3 });
            }
        }
    };

    const handleUpdateLink = async (formData) => {
        if (!selectedLink) return;
        try {
            await updateLinkMutation({
                variables: {
                    input: {
                        currentLabel: selectedLink.label || selectedLink.description,
                        currentUrl: selectedLink.url,
                        resourceUrn: selectedLink.associatedUrn || entityUrn,
                        label: formData.label,
                        linkUrl: formData.url,
                        settings: { showInAssetPreview },
                    },
                },
            });
            message.success({ content: 'Link Updated', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Error updating link: \n ${e.message || ''}`, duration: 2 });
            }
        }
        refetch?.();
    };

    return { handleDeleteLink, handleAddLink, handleUpdateLink, showInAssetPreview, setShowInAssetPreview };
}
