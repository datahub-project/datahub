import { useApolloClient } from '@apollo/client';
import { useContext, useEffect, useRef, useState } from 'react';

import { FetchStatus, LineageEntity, LineageNodesContext, setDefault } from '@app/lineageV3/common';

import {
    GetDataProductEntitiesForLineageDocument,
    GetDataProductEntitiesForLineageQuery,
} from '@graphql/dataProduct.generated';
import { Entity, EntityType, LineageDirection } from '@types';

// Cap assets shown per DP inside the Domain view. The Domain view is a high-level overview;
// surfacing every asset of every DP would explode the layout. 25 matches the resolver-side
// `perMemberCount` and DOMAIN_ENTITIES_PAGE_SIZE conventions used elsewhere in the stack.
const ASSETS_PER_DP_CAP = 25;
// Bound concurrent per-DP fetches to keep the page responsive when a Domain has many DPs.
const MAX_PARALLEL_DP_FETCHES = 4;

/**
 * Fetches the assets belonging to each member DataProduct of the current Domain and registers
 * them as nodes with {@code parentDataProduct} set so {@code computeDomainGraph} can nest them
 * inside their DP's bounding box.
 *
 * Runs after {@link useFetchDomainEntities} has populated the DP roster. Each DP triggers a
 * single capped query against `getDataProductEntitiesForLineage`; DPs already processed in a
 * prior tick are skipped via a per-instance set.
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

        // Optimistically claim the DPs we're about to fetch so a re-entrant render doesn't
        // re-queue them. Failures still resolve (we just won't see their assets), and the user
        // can refresh to retry.
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
): Promise<boolean> {
    try {
        const result = await client.query<GetDataProductEntitiesForLineageQuery>({
            query: GetDataProductEntitiesForLineageDocument,
            variables: { urn: dpUrn, start: 0, count: ASSETS_PER_DP_CAP },
            fetchPolicy: 'cache-first',
        });
        const searchResults = result.data?.dataProduct?.entities?.searchResults ?? [];
        let added = false;
        searchResults.forEach((row) => {
            if (!row?.entity) return;
            const wasNew = !nodes.has(row.entity.urn);
            const node = setDefault(nodes, row.entity.urn, makeAssetNode(row.entity));
            // Only adopt the asset into the current DP if it isn't already pinned to the source
            // Domain (i.e. a directly-tagged Domain asset that also happens to belong to a DP).
            // Source-Domain pinning takes precedence so the asset stays at the Domain level
            // rather than getting shoved inside a DP bbox.
            if (node.parentDomain !== rootUrn) {
                node.parentDataProduct = dpUrn;
            }
            if (wasNew) added = true;
        });
        return added;
    } catch (err) {
        // Swallow per-DP failures so one bad DP doesn't take the whole view down. The user will
        // simply see fewer nested assets for that DP.
        // eslint-disable-next-line no-console
        console.warn('Failed to fetch DP assets for nested Domain view', { dpUrn, err });
        return false;
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
    const counts: number[] = [];

    async function worker(): Promise<void> {
        for (;;) {
            const dpUrn = queue.shift();
            if (dpUrn === undefined) return;
            // Sequential per-worker by design — concurrency is provided by running multiple
            // workers in parallel below. Single worker awaiting in a loop is exactly the
            // intended bounded-concurrency pattern.
            // eslint-disable-next-line no-await-in-loop
            const added = await fetchDpAssets(client, rootUrn, dpUrn, nodes);
            if (added) counts.push(1);
        }
    }

    const workerCount = Math.min(MAX_PARALLEL_DP_FETCHES, dpUrns.length);
    const workers: Promise<void>[] = [];
    for (let i = 0; i < workerCount; i += 1) workers.push(worker());
    await Promise.all(workers);
    if (counts.length > 0) bumpNodeVersion();
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
