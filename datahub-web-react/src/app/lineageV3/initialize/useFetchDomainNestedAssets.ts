import { useApolloClient } from '@apollo/client';
import { useContext, useEffect, useRef, useState } from 'react';

import { FetchStatus, LineageEntity, LineageNodesContext, setDefault } from '@app/lineageV3/common';

import {
    GetDataProductEntitiesForLineageDocument,
    GetDataProductEntitiesForLineageQuery,
} from '@graphql/dataProduct.generated';
import { Entity, EntityType, LineageDirection } from '@types';

// Matches the resolver-side perMemberCount default and DOMAIN_ENTITIES_PAGE_SIZE used elsewhere
// in the stack — keeps the Domain view's per-DP footprint bounded.
const ASSETS_PER_DP_CAP = 25;
const MAX_PARALLEL_DP_FETCHES = 4;

/**
 * Fetches each member DataProduct's first {@link ASSETS_PER_DP_CAP} assets and stamps them with
 * {@code parentDataProduct} so {@code computeDomainGraph} nests them inside their DP bbox.
 */
export default function useFetchDomainNestedAssets(): boolean {
    const client = useApolloClient();
    const { rootUrn, rootType, nodes, nodeVersion, setNodeVersion } = useContext(LineageNodesContext);
    const fetchedDpUrns = useRef<Set<string>>(new Set());
    const [initialized, setInitialized] = useState(false);

    const enabled = rootType === EntityType.Domain;

    useEffect(() => {
        fetchedDpUrns.current = new Set();
        setInitialized(false);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [rootUrn]);

    useEffect(() => {
        if (!enabled) {
            setInitialized(true);
            return;
        }

        const candidateDps: string[] = [];
        nodes.forEach((node) => {
            if (node.parentDomain !== rootUrn) return;
            if (node.type !== EntityType.DataProduct) return;
            if (fetchedDpUrns.current.has(node.urn)) return;
            candidateDps.push(node.urn);
        });

        if (candidateDps.length === 0) {
            setInitialized(true);
            return;
        }

        // Claim the DPs up front so a re-entrant render doesn't re-queue them mid-fetch.
        candidateDps.forEach((urn) => fetchedDpUrns.current.add(urn));

        runBoundedFetch(client, rootUrn, candidateDps, nodes, () => setNodeVersion((v) => v + 1))
            .catch(() => undefined)
            .finally(() => setInitialized(true));
    }, [enabled, rootUrn, nodes, nodeVersion, client, setNodeVersion]);

    if (!enabled) return true;
    return initialized;
}

async function fetchDpAssets(
    client: ReturnType<typeof useApolloClient>,
    rootUrn: string,
    dpUrn: string,
    nodes: Map<string, LineageEntity>,
): Promise<number> {
    try {
        const result = await client.query<GetDataProductEntitiesForLineageQuery>({
            query: GetDataProductEntitiesForLineageDocument,
            variables: { urn: dpUrn, start: 0, count: ASSETS_PER_DP_CAP },
            fetchPolicy: 'cache-first',
        });
        const searchResults = result.data?.dataProduct?.entities?.searchResults ?? [];
        let added = 0;
        searchResults.forEach((row) => {
            if (!row?.entity) return;
            const wasNew = !nodes.has(row.entity.urn);
            const node = setDefault(nodes, row.entity.urn, makeAssetNode(row.entity));
            // Source-Domain pinning takes precedence: a directly-tagged Domain asset stays at the
            // Domain level rather than being adopted into a DP bbox.
            if (node.parentDomain !== rootUrn) {
                node.parentDataProduct = dpUrn;
            }
            if (wasNew) added += 1;
        });
        return added;
    } catch (err) {
        // eslint-disable-next-line no-console
        console.warn('Failed to fetch DP assets for nested Domain view', { dpUrn, err });
        return 0;
    }
}

async function runBoundedFetch(
    client: ReturnType<typeof useApolloClient>,
    rootUrn: string,
    dpUrns: string[],
    nodes: Map<string, LineageEntity>,
    bumpNodeVersion: () => void,
): Promise<void> {
    const queue = [...dpUrns];

    async function worker(): Promise<number> {
        let added = 0;
        for (;;) {
            const dpUrn = queue.shift();
            if (dpUrn === undefined) return added;
            // Sequential per worker; concurrency comes from running MAX_PARALLEL_DP_FETCHES of them.
            // eslint-disable-next-line no-await-in-loop
            added += await fetchDpAssets(client, rootUrn, dpUrn, nodes);
        }
    }

    const workerCount = Math.min(MAX_PARALLEL_DP_FETCHES, dpUrns.length);
    const workers: Promise<number>[] = [];
    for (let i = 0; i < workerCount; i += 1) workers.push(worker());
    const counts = await Promise.all(workers);
    if (counts.some((c) => c > 0)) bumpNodeVersion();
}

function makeAssetNode({ urn, type }: Entity): LineageEntity {
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
