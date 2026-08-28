import isEqual from 'lodash/isEqual';
import { useEffect } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import {
    DataProductsEntityData,
    useMaybeDataProductsEntityContext,
} from '@app/dataProducts/context/DataProductsEntityContext';
import usePrevious from '@app/shared/usePrevious';

import { EntityType } from '@types';

type DataProductsEntitySource = Pick<GenericEntityProperties, 'urn'> & {
    parentDataProducts?: Array<{ urn: string } | null> | null;
};

function toDataProductsEntityData(
    entityData: DataProductsEntitySource,
    entityType: EntityType,
): DataProductsEntityData | null {
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
 * Pushes the parentDataProducts chain into DataProductsEntityContext so the sidebar can
 * self-expand to the currently-viewed entity.
 */
export function useUpdateDataProductsEntityDataOnChange(
    entityData: GenericEntityProperties | null,
    entityType: EntityType,
): void {
    const dataProductsContext = useMaybeDataProductsEntityContext();
    const setEntityData = dataProductsContext?.setEntityData;
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

        const next = toDataProductsEntityData(entityData, entityType);
        if (!next) {
            return;
        }

        setEntityData(next);
    }, [entityData, entityType, previousEntityData, setEntityData]);
}
