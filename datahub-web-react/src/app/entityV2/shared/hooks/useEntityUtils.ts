import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { capitalizeFirstLetterOnly } from '@src/app/shared/textUtil';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { CorpUser, Entity } from '@src/types.generated';
import { useCallback } from 'react';
import { getPlatformName as getPlatformNameUtil } from '../utils';

export default function useEntityUtils() {
    const entityRegistry = useEntityRegistryV2();

    const getGenericProperties = useCallback(
        (entity: Entity): GenericEntityProperties | null => {
            return entityRegistry.getGenericEntityProperties(entity.type, entity);
        },
        [entityRegistry],
    );

    const getDisplayName = useCallback(
        (entity: Entity) => entityRegistry.getDisplayName(entity.type, entity),
        [entityRegistry],
    );

    const getDisplayType = useCallback(
        (entity: Entity, properties?: GenericEntityProperties | null) => {
            const finalProperties = properties ?? getGenericProperties(entity);
            const subtype = finalProperties?.subTypes?.typeNames?.[0];
            const entityName = entityRegistry.getEntityName(entity.type);
            const displayType = capitalizeFirstLetterOnly((subtype ?? entityName)?.toLocaleLowerCase());
            return displayType;
        },
        [entityRegistry, getGenericProperties],
    );

    const getUserPictureLink = useCallback((user: CorpUser) => user?.editableProperties?.pictureLink, []);
    const getUserName = useCallback((user: CorpUser) => user?.username, []);

    const getPlatformLogoUrl = useCallback(
        (entity: Entity, properties?: GenericEntityProperties | null) => {
            const finalProperties = properties ?? getGenericProperties(entity);
            return finalProperties?.platform?.properties?.logoUrl;
        },
        [getGenericProperties],
    );

    const getPlatformName = useCallback(
        (entity: Entity, properties?: GenericEntityProperties | null) => {
            const finalProperties = properties ?? getGenericProperties(entity);
            return getPlatformNameUtil(finalProperties);
        },
        [getGenericProperties],
    );

    return {
        entityRegistry,
        getGenericProperties,
        getDisplayName,
        getDisplayType,
        getUserPictureLink,
        getUserName,
        getPlatformLogoUrl,
        getPlatformName,
    };
}
