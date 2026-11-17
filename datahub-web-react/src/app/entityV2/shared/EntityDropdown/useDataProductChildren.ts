import { useCallback, useMemo } from 'react';

import { GenericEntityProperties } from '@app/entityV2/shared/types';

import { useGetDataProductChildrenCountLazyQuery } from '@graphql/dataProduct.generated';
import { EntityType } from '@types';

/**
 * Data product cards (search results, a domain's Data Products tab) are built from list queries that
 * do not carry the child count the profile page has. Fetch it when the menu opens, so Delete can be
 * disabled there too without an extra search for every card on the page.
 */
export default function useDataProductChildren(
    urn: string,
    entityType: EntityType,
    entityData: GenericEntityProperties | null,
) {
    const needsLookup = entityType === EntityType.DataProduct && entityData?.children == null;
    const [fetchChildren, { data }] = useGetDataProductChildrenCountLazyQuery({ fetchPolicy: 'network-only' });

    const onMenuOpenChange = useCallback(
        (open: boolean) => {
            if (open && needsLookup) fetchChildren({ variables: { urn } });
        },
        [needsLookup, fetchChildren, urn],
    );

    const total = data?.dataProduct?.childDataProducts?.total;
    const entityDataWithChildren = useMemo<GenericEntityProperties | null>(
        () =>
            needsLookup && total !== undefined ? { ...entityData, children: { relationships: [], total } } : entityData,
        [needsLookup, total, entityData],
    );

    return { entityData: entityDataWithChildren, onMenuOpenChange };
}
