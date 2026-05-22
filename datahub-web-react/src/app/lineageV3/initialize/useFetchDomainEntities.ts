import { useContext, useState } from 'react';

import { FetchStatus, LineageEntity, LineageNodesContext, setDefault } from '@app/lineageV3/common';

import { useGetDomainEntitiesForLineageQuery } from '@graphql/domain.generated';
import { Entity, LineageDirection } from '@types';

const DOMAIN_ENTITIES_PAGE_SIZE = 50;

/**
 * Fetches the entities belonging to a Domain (DataProducts and any directly-tagged assets) and
 * registers them as lineage nodes. Mirrors `useFetchDataProductEntities` but stamps each member
 * with `parentDomain` instead of `parentDataProduct` so `computeDomainGraph` can identify them.
 *
 * Member nodes start at FetchStatus.UNNEEDED for both directions: the Domain view aggregates
 * lineage via the `domainLineage` resolver, so we don't trigger per-member lineage walks here.
 */
export default function useFetchDomainEntities(): boolean {
    const { rootUrn, nodes, setNodeVersion } = useContext(LineageNodesContext);
    const [start, setStart] = useState(0);
    const [initialized, setInitialized] = useState(false);

    useGetDomainEntitiesForLineageQuery({
        variables: {
            urn: rootUrn,
            start,
            count: DOMAIN_ENTITIES_PAGE_SIZE,
        },
        onCompleted: (data) => {
            let addedNode = false;

            const entities = data.domain?.entities;
            entities?.searchResults?.forEach((result) => {
                if (!result?.entity) return;
                addedNode = addedNode || !nodes.has(result.entity.urn);
                const node = setDefault(nodes, result.entity.urn, makeEntityNode(result.entity));
                node.parentDomain = rootUrn;
            });

            if (entities?.total && entities.total > start + DOMAIN_ENTITIES_PAGE_SIZE) {
                setStart((prev) => prev + DOMAIN_ENTITIES_PAGE_SIZE);
            }

            setInitialized(true);

            if (addedNode) setNodeVersion((version) => version + 1);
        },
    });

    return initialized;
}

function makeEntityNode({ urn, type }: Entity): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        isExpanded: {
            [LineageDirection.Upstream]: false,
            [LineageDirection.Downstream]: false,
        },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.UNNEEDED,
            [LineageDirection.Downstream]: FetchStatus.UNNEEDED,
        },
        filters: {
            [LineageDirection.Upstream]: { limit: undefined, facetFilters: new Map() },
            [LineageDirection.Downstream]: { limit: undefined, facetFilters: new Map() },
        },
    };
}
