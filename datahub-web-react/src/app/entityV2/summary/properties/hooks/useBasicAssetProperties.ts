import { useMemo } from 'react';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import {
    CREATED_PROPERTY,
    DOMAIN_PROPERTY,
    LAST_INGESTED_PROPERTY,
    LAST_MODIFIED_PROPERTY,
    OWNERS_PROPERTY,
    SEMANTIC_MODEL_PROPERTY,
    TAGS_PROPERTY,
    TERMS_PROPERTY,
} from '@app/entityV2/summary/properties/constants';

import { EntityType } from '@types';

export default function useBasicAssetProperties() {
    const { entityType } = useEntityContext();

    const basicAssetProperties = useMemo(() => {
        switch (entityType) {
            case EntityType.Domain:
                return [CREATED_PROPERTY, OWNERS_PROPERTY];
            case EntityType.GlossaryTerm:
                return [CREATED_PROPERTY, OWNERS_PROPERTY, DOMAIN_PROPERTY];
            case EntityType.GlossaryNode:
                return [CREATED_PROPERTY, OWNERS_PROPERTY];
            case EntityType.DataProduct:
                return [CREATED_PROPERTY, OWNERS_PROPERTY, DOMAIN_PROPERTY, TAGS_PROPERTY, TERMS_PROPERTY];
            case EntityType.Dataset:
                return [CREATED_PROPERTY, OWNERS_PROPERTY, DOMAIN_PROPERTY, TAGS_PROPERTY, TERMS_PROPERTY];
            case EntityType.SemanticModel:
                return [LAST_INGESTED_PROPERTY, DOMAIN_PROPERTY, OWNERS_PROPERTY, TERMS_PROPERTY];
            case EntityType.Metric:
                return [CREATED_PROPERTY, LAST_MODIFIED_PROPERTY, OWNERS_PROPERTY, SEMANTIC_MODEL_PROPERTY];
            case EntityType.Document:
                // Documents don't have assetSettings aspect yet - hide add button until backend supports it
                return [];
            default:
                return [];
        }
    }, [entityType]);

    return basicAssetProperties;
}
