import React from 'react';
import { Helmet } from 'react-helmet';
import { useEntityData } from '../entity/shared/EntityContext';
import { useEntityRegistry } from '../useEntityRegistry';
import { capitalizeFirstLetterOnly } from './textUtil';

export const EntityHead = () => {
    const entityRegistry = useEntityRegistry();
    const entityData = useEntityData();

    const entityDisplayName = entityRegistry.getDisplayName(entityData.entityType, entityData.entityData);
    const type =
        capitalizeFirstLetterOnly(entityData?.entityData?.subTypes?.typeNames?.[0]) ||
        entityRegistry.getEntityName(entityData.entityType);

    return (
        <Helmet>
            <title>
                {entityDisplayName} | {type}
            </title>
        </Helmet>
    );
};
