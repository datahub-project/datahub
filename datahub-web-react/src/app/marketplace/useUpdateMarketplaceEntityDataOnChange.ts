import isEqual from 'lodash/isEqual';
import { useEffect } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { MarketplaceEntityData, useMarketplaceEntityContext } from '@app/marketplace/context/MarketplaceEntityContext';
import usePrevious from '@app/shared/usePrevious';

import { EntityType } from '@types';

/**
 * Called from EntityProfile whenever the entity data for a data-product profile page changes.
 * Pushes the parentDataProducts chain into MarketplaceEntityContext so the sidebar can
 * self-expand to the currently-viewed entity.
 */
export function useUpdateMarketplaceEntityDataOnChange(
    entityData: GenericEntityProperties | null,
    entityType: EntityType,
): void {
    const { setEntityData } = useMarketplaceEntityContext();
    const previousEntityData = usePrevious(entityData);

    useEffect(() => {
        if (entityType !== EntityType.DataProduct || isEqual(entityData, previousEntityData)) {
            return;
        }

        if (!entityData) {
            setEntityData(null);
            return;
        }

        const raw = entityData as any;
        const next: MarketplaceEntityData = {
            urn: raw.urn ?? '',
            entityType,
            parentDataProducts: Array.isArray(raw.parentDataProducts)
                ? raw.parentDataProducts.map((p: any) => ({ urn: p.urn }))
                : null,
        };

        setEntityData(next);
    });
}
