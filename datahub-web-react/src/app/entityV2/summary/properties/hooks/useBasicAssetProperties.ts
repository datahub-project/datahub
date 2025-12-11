/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import {
    CREATED_PROPERTY,
    DOMAIN_PROPERTY,
    OWNERS_PROPERTY,
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
            default:
                return [];
        }
    }, [entityType]);

    return basicAssetProperties;
}
