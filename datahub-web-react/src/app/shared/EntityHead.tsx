import React from 'react';
import { Helmet } from 'react-helmet';
import { useEntityData } from '../entity/shared/EntityContext';
import { useEntityRegistry } from '../useEntityRegistry';
import { capitalizeFirstLetterOnly } from './textUtil';

export const EntityHead = () => {
    const entityRegistry = useEntityRegistry();
    const { entityType, entityData } = useEntityData();

    if (!entityData) {
        return null;
    }

    const entityDisplayName = entityRegistry.getDisplayName(entityType, entityData);
    const type =
        capitalizeFirstLetterOnly(entityData?.subTypes?.typeNames?.[0]) || entityRegistry.getEntityName(entityType);

    return (
        <Helmet>
            <title>
                {entityDisplayName} | {type}
            </title>
        </Helmet>
    );
};
