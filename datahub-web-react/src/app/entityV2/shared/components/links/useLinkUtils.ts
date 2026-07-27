import { toast } from '@components';
import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { LinkFormData } from '@app/entityV2/shared/components/links/types';
import { getGeneralizedLinkFormDataFromFormData } from '@app/entityV2/shared/components/links/utils';

import { useAddLinkMutation, useRemoveLinkMutation, useUpdateLinkMutation } from '@graphql/mutations.generated';
import { InstitutionalMemoryMetadata } from '@types';

export function useLinkUtils(selectedLink: InstitutionalMemoryMetadata | null = null) {
    const { t } = useTranslation('entity.shared.components');
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

    const handleDeleteLink = async (link?: InstitutionalMemoryMetadata | null) => {
        const linkToDelete = link ?? selectedLink;
        if (!linkToDelete) {
            return;
        }
        try {
            await removeLinkMutation({
                variables: {
                    input: {
                        linkUrl: linkToDelete.url,
                        label: linkToDelete.label || linkToDelete.description,
                        resourceUrn: linkToDelete.associatedUrn || entityUrn,
                    },
                },
            });
            toast.success(t('links.removed'));
            analytics.event({
                type: EventType.EntityActionEvent,
                entityType,
                entityUrn: mutationUrn,
                actionType: EntityActionType.DeleteLink,
            });
        } catch (e: unknown) {
            if (e instanceof Error) {
                toast.error(t('links.removeError', { message: e.message || '' }));
            }
        }
        refetch?.();
    };

    const handleAddLink = async (formValues: LinkFormData) => {
        try {
            const generalizedFormValues = getGeneralizedLinkFormDataFromFormData(formValues);
            await addLinkMutation({
                variables: {
                    input: {
                        linkUrl: generalizedFormValues.url,
                        label: generalizedFormValues.label,
                        resourceUrn: mutationUrn,
                        settings: { showInAssetPreview },
                    },
                },
            });
            toast.success(t('links.added'));
            analytics.event({
                type: EventType.EntityActionEvent,
                entityType,
                entityUrn: mutationUrn,
                actionType: EntityActionType.AddLink,
            });
            refetch?.();
        } catch (e: unknown) {
            if (e instanceof Error) {
                toast.error(t('links.addError', { message: e.message || '' }));
            }
        }
    };

    const handleUpdateLink = async (formData: LinkFormData) => {
        if (!selectedLink) return;
        try {
            const generalizedFormValues = getGeneralizedLinkFormDataFromFormData(formData);
            await updateLinkMutation({
                variables: {
                    input: {
                        currentLabel: selectedLink.label || selectedLink.description,
                        currentUrl: selectedLink.url,
                        resourceUrn: selectedLink.associatedUrn || entityUrn,
                        label: generalizedFormValues.label,
                        linkUrl: generalizedFormValues.url,
                        settings: { showInAssetPreview },
                    },
                },
            });
            toast.success(t('links.updated'));
            analytics.event({
                type: EventType.EntityActionEvent,
                entityType,
                entityUrn: mutationUrn,
                actionType: EntityActionType.UpdateLinks,
            });
        } catch (e: unknown) {
            if (e instanceof Error) {
                toast.error(t('links.updateError', { message: e.message || '' }));
            }
        }
        refetch?.();
    };

    return { handleDeleteLink, handleAddLink, handleUpdateLink, showInAssetPreview, setShowInAssetPreview };
}
