import { useMemo } from 'react';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import {
    CREATED_PROPERTY,
    DOMAIN_PROPERTY,
    OWNERS_PROPERTY,
    TAGS_PROPERTY,
    TERMS_PROPERTY,
} from '@app/entityV2/summary/properties/constants';
import { AssetProperty } from '@app/entityV2/summary/properties/types';

import { EntityType } from '@types';

export default function useAvailableAssetProperties() {
    const { entityType } = useEntityContext();

    const availableProperties = useMemo(() => {
        switch (entityType) {
            case EntityType.Domain:
                return [CREATED_PROPERTY, OWNERS_PROPERTY];
            case EntityType.GlossaryTerm:
                return [CREATED_PROPERTY, OWNERS_PROPERTY, DOMAIN_PROPERTY];
            case EntityType.DataProduct:
                return [CREATED_PROPERTY, OWNERS_PROPERTY, DOMAIN_PROPERTY, TAGS_PROPERTY, TERMS_PROPERTY];
            default:
                return [];
        }
    }, [entityType]);

    // TODO: implement getting of structured properties
    const availableStructuredProperties: AssetProperty[] = useMemo(() => [], []);

    return { availableProperties, availableStructuredProperties };
}
