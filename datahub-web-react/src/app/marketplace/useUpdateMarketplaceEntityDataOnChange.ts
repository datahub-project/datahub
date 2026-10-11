import isEqual from 'lodash/isEqual';
import { useEffect } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import {
    MarketplaceEntityData,
    useMaybeMarketplaceEntityContext,
} from '@app/marketplace/context/MarketplaceEntityContext';
import usePrevious from '@app/shared/usePrevious';

import { EntityType } from '@types';

type MarketplaceEntitySource = Pick<GenericEntityProperties, 'urn'> & {
    parentDataProducts?: Array<{ urn: string } | null> | null;
};

function toMarketplaceEntityData(
    entityData: MarketplaceEntitySource,
    entityType: EntityType,
): MarketplaceEntityData | null {
    if (!entityData.urn) {
        return null;
    }

    return {
        urn: entityData.urn,
        entityType,
        parentDataProducts: Array.isArray(entityData.parentDataProducts)
            ? entityData.parentDataProducts.filter((p): p is { urn: string } => !!p?.urn).map((p) => ({ urn: p.urn }))
            : null,
    };
}

/**
 * Called from EntityProfile whenever the entity data for a data-product profile page changes.
 * Pushes the parentDataProducts chain into MarketplaceEntityContext so the sidebar can
 * self-expand to the currently-viewed entity.
 */
export function useUpdateMarketplaceEntityDataOnChange(
    entityData: GenericEntityProperties | null,
    entityType: EntityType,
): void {
    const marketplaceContext = useMaybeMarketplaceEntityContext();
    const setEntityData = marketplaceContext?.setEntityData;
    const previousEntityData = usePrevious(entityData);

    useEffect(() => {
        if (!setEntityData) {
            return;
        }
        if (entityType !== EntityType.DataProduct || isEqual(entityData, previousEntityData)) {
            return;
        }

        if (!entityData) {
            setEntityData(null);
            return;
        }

        const next = toMarketplaceEntityData(entityData, entityType);
        if (!next) {
            return;
        }

        setEntityData(next);
    }, [entityData, entityType, previousEntityData, setEntityData]);
}
