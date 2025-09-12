import { useEffect, useState } from 'react';

import { useEntityData, useEntityUpdate, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { GenericEntityUpdate } from '@app/entity/shared/types';
import { getAssetDescriptionDetails } from '@app/entityV2/shared/tabs/Documentation/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';

export function useDescriptionUtils() {
    const { entityData, entityType } = useEntityData();
    const entityRegistry = useEntityRegistryV2();
    const mutationUrn = useMutationUrn();
    const refetch = useRefetch();

    const [updateDescriptionMutation] = useUpdateDescriptionMutation();

    const { displayedDescription } = getAssetDescriptionDetails({
        entityProperties: entityData,
    });

    const [updatedDescription, setUpdatedDescription] = useState<string>(displayedDescription);
    const updateEntity = useEntityUpdate<GenericEntityUpdate>();

    useEffect(() => {
        setUpdatedDescription(displayedDescription);
    }, [displayedDescription]);

    const updateDescriptionLegacy = () => {
        return updateEntity?.({
            variables: { urn: mutationUrn, input: { editableProperties: { description: updatedDescription } } },
        });
    };

    const updateDescription = () => {
        return updateDescriptionMutation({
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
        refetch();
    };

    const emptyDescriptionText = `Write a description for this ${entityRegistry.getEntityName(entityType)?.toLowerCase()}`;

    return {
        displayedDescription,
        updatedDescription,
        setUpdatedDescription,
        handleDescriptionUpdate,
        emptyDescriptionText,
    };
}
