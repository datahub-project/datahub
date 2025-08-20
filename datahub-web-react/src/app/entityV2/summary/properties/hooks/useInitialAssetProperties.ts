import { useMemo } from 'react';

import {
    CREATED_PROPERTY,
    DOMAIN_PROPERTY,
    OWNERS_PROPERTY,
    TAGS_PROPERTY,
    TERMS_PROPERTY,
} from '@app/entityV2/summary/properties/constants';
import { AssetProperty } from '@app/entityV2/summary/properties/types';

import { EntityType } from '@types';

interface Response {
    properties: AssetProperty[];
    loading: boolean;
}

export default function useInitialAssetProperties(entityType: EntityType): Response {
    const defaultProperties: AssetProperty[] = useMemo(() => {
        switch (entityType) {
            case EntityType.Domain:
                return [CREATED_PROPERTY, OWNERS_PROPERTY];
            case EntityType.GlossaryTerm:
                return [CREATED_PROPERTY, OWNERS_PROPERTY, DOMAIN_PROPERTY];
            case EntityType.DataProduct:
                return [CREATED_PROPERTY, DOMAIN_PROPERTY, OWNERS_PROPERTY, TAGS_PROPERTY, TERMS_PROPERTY];
            default:
                return [];
        }
    }, [entityType]);

    // TODO: implement getting properties from an entity
    const propertiesFromEntity: AssetProperty[] | undefined = useMemo(() => undefined, []);

    const properties = useMemo(
        () => propertiesFromEntity || defaultProperties,
        [propertiesFromEntity, defaultProperties],
    );

    return {
        properties,
        loading: false,
    };
}
