import { useEffect, useState } from 'react';

import { useEntityData, useEntityUpdate, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { GenericEntityUpdate } from '@app/entity/shared/types';
import { getAssetDescriptionDetails } from '@app/entityV2/shared/tabs/Documentation/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';

export function useDescriptionUtils() {
    const { entityData, urn, entityType } = useEntityData();
    const entityRegistry = useEntityRegistryV2();
    const mutationUrn = useMutationUrn();
    const refetch = useRefetch();

    const [updateDescriptionMutation] = useUpdateDescriptionMutation();

    const { displayedDescription } = getAssetDescriptionDetails({
        entityProperties: entityData,
    });

    const [initialDescription, setInitialDescription] = useState<string>(displayedDescription);
    const [updatedDescription, setUpdatedDescription] = useState<string>(displayedDescription);
    const [isEditing, setIsEditing] = useState(false);
    const updateEntity = useEntityUpdate<GenericEntityUpdate>();

    useEffect(() => {
        setInitialDescription(displayedDescription);
        setUpdatedDescription(displayedDescription);
    }, [displayedDescription]);

    // Reset isEditing when asset changes
    useEffect(() => {
        setIsEditing(false);
    }, [urn]);

    const updateDescriptionLegacy = () => {
        return updateEntity?.({
            variables: { urn: mutationUrn, input: { editableProperties: { description: updatedDescription } } },
        });
    };

    const updateDescription = () => {
        updateDescriptionMutation({
            variables: {
                input: {
                    description: updatedDescription,
                    resourceUrn: mutationUrn,
                },
            },
        });
    };

    const handleDescriptionUpdate = async () => {
        if (updateEntity) {
            // Use the legacy update description path.
            await updateDescriptionLegacy();
        } else {
            // Use the new update description path.
            await updateDescription();
        }
        setTimeout(() => {
            refetch();
        }, 2000);
        setIsEditing(false);
    };

    const handleCancel = () => {
        setIsEditing(false);
        setUpdatedDescription(initialDescription);
    };

    const emptyDescriptionText = `Write a description for this ${entityRegistry.getEntityName(entityType)?.toLowerCase()}`;

    return {
        isEditing,
        setIsEditing,
        initialDescription,
        updatedDescription,
        setUpdatedDescription,
        handleDescriptionUpdate,
        handleCancel,
        emptyDescriptionText,
    };
}
