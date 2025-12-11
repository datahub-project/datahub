/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useState } from 'react';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityData, useEntityUpdate, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { GenericEntityUpdate } from '@app/entity/shared/types';
import { getAssetDescriptionDetails } from '@app/entityV2/shared/tabs/Documentation/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';

export function useDescriptionUtils() {
    const { entityData, entityType, urn } = useEntityData();
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
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.UpdateDescription,
            entityType,
            entityUrn: urn,
        });
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
