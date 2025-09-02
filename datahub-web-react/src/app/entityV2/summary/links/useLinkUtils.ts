import { message } from 'antd';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';

import { useAddLinkMutation, useRemoveLinkMutation } from '@graphql/mutations.generated';
import { InstitutionalMemoryMetadata } from '@types';

export function useLinkUtils() {
    const { urn: entityUrn, entityType } = useEntityData();
    const refetch = useRefetch();
    const mutationUrn = useMutationUrn();
    const user = useUserContext();

    const [removeLinkMutation] = useRemoveLinkMutation();
    const [addLinkMutation] = useAddLinkMutation();

    const handleDeleteLink = async (link: InstitutionalMemoryMetadata) => {
        try {
            await removeLinkMutation({
                variables: { input: { linkUrl: link.url, resourceUrn: link.associatedUrn || entityUrn } },
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
        if (user?.urn) {
            try {
                await addLinkMutation({
                    variables: {
                        input: { linkUrl: formValues.url, label: formValues.label, resourceUrn: mutationUrn },
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
        } else {
            message.error({ content: `Error adding link: no user`, duration: 2 });
        }
    };

    return { handleDeleteLink, handleAddLink };
}
