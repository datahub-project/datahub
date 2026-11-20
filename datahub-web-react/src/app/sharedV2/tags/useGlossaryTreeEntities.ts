import { useApolloClient } from '@apollo/client';
import { useCallback, useMemo, useState } from 'react';

import { DEFAULT_GLOSSARY_CHILDREN_COUNT, getGlossaryChildrenScrollInput } from '@app/glossaryV2/utils';

import { useGetRootGlossaryNodesQuery, useGetRootGlossaryTermsQuery } from '@graphql/glossary.generated';
import {
    ScrollAcrossEntitiesDocument,
    ScrollAcrossEntitiesQuery,
    ScrollAcrossEntitiesQueryVariables,
} from '@graphql/search.generated';
import { Entity, EntityType, GlossaryNode, GlossaryTerm, ParentNodesResult } from '@types';

interface UseGlossaryTreeEntitiesResult {
    /** Flat list of root entities + the children of every currently-expanded node. Each entity has a
     * synthetic `parentNodes` chain attached, so it can be fed straight into `useTermTreeOptions`. */
    entities: Entity[];
    /** URN → entity map of every entity the tree has ever surfaced (roots + children of any node
     * that has been expanded at least once). Stays populated after a collapse, so the modal can
     * still resolve display names for selected URNs whose tree branch is no longer rendered. */
    entityCache: Record<string, Entity>;
    /** URNs of nodes whose children have been requested (drives the rendered expand state). */
    expandedNodes: Set<string>;
    /** URNs of nodes with an in-flight child fetch. Used by the modal to render an inline loading
     * row directly under the expanded node (rather than a global loader at the top). */
    fetchingNodes: Set<string>;
    /** Expand a node: triggers a lazy `scrollAcrossEntities` fetch (if not already cached) and adds
     * the node to `expandedNodes`. */
    expandNode: (nodeUrn: string) => void;
    /** Collapse a node: just removes the node from `expandedNodes`; the fetched children stay cached
     * so re-expanding is instant. */
    collapseNode: (nodeUrn: string) => void;
    /** True only while the initial root nodes/terms queries are in flight. Per-node child fetches
     * are surfaced separately via `fetchingNodes` so they can be rendered inline. */
    isLoading: boolean;
}

/** Attach a synthetic `parentNodes` chain to an entity. The chain is direct-parent-first to match
 * the GraphQL convention used elsewhere (`childGlossaryTerm`, etc.). */
function withParentChain<E extends Entity>(entity: E, parentChain: GlossaryNode[]): E {
    const parentNodes: ParentNodesResult = { count: parentChain.length, nodes: parentChain };
    return { ...entity, parentNodes } as E;
}

/**
 * Owns the data layer for `AddTermsModal`'s browse experience. Fetches root nodes + root terms on
 * mount, then lazy-fetches each node's children on demand when the user expands it (rather than
 * walking the whole glossary up front — there can be thousands of terms).
 *
 * The returned `entities` array is flat and includes synthetic `parentNodes` chains derived from
 * the expand path, so `useTermTreeOptions` can group everything into a tree without needing the
 * GraphQL fragments to ship a full ancestor chain.
 *
 * Cached children stay around after a collapse so re-expanding is free. Mirrors the data flow of
 * the existing `GlossaryBrowser` sidebar (`useGlossaryChildren`) so the two views stay consistent.
 */
export function useGlossaryTreeEntities(): UseGlossaryTreeEntitiesResult {
    const apolloClient = useApolloClient();

    const { data: rootNodesData, loading: rootNodesLoading } = useGetRootGlossaryNodesQuery();
    const { data: rootTermsData, loading: rootTermsLoading } = useGetRootGlossaryTermsQuery();

    // children of each expanded node, keyed by parent node URN; populated by `expandNode`.
    const [childrenByNode, setChildrenByNode] = useState<Record<string, Entity[]>>({});
    // visible-expand state — what carets are flipped open.
    const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
    // in-flight fetches, used to avoid double-fetch when expandNode is called twice quickly.
    const [fetchingNodes, setFetchingNodes] = useState<Set<string>>(new Set());
    // parent chain (root-to-direct-parent) for any node we've seen, used when synthesizing
    // `parentNodes` on its future children. Roots map to an empty chain.
    const [chainByNode, setChainByNode] = useState<Record<string, GlossaryNode[]>>({});

    const rootNodes = useMemo<GlossaryNode[]>(
        () => (rootNodesData?.getRootGlossaryNodes?.nodes ?? []).map((n) => withParentChain(n as GlossaryNode, [])),
        [rootNodesData],
    );
    const rootTerms = useMemo<GlossaryTerm[]>(
        () => (rootTermsData?.getRootGlossaryTerms?.terms ?? []).map((t) => withParentChain(t as GlossaryTerm, [])),
        [rootTermsData],
    );

    // Persistent URN → entity map derived from the underlying state (roots + every fetched child
    // bucket, which stays in `childrenByNode` after a collapse). Two consumers:
    //   - exposed as `entityCache` for the modal's chip-strip display name resolution.
    //   - used internally by `expandNode` to look up the parent's entity when building the chain
    //     for its newly-fetched children.
    const entityCache = useMemo<Record<string, Entity>>(() => {
        const cache: Record<string, Entity> = {};
        rootNodes.forEach((n) => {
            cache[n.urn] = n;
        });
        rootTerms.forEach((t) => {
            cache[t.urn] = t;
        });
        Object.values(childrenByNode).forEach((bucket) => {
            bucket.forEach((e) => {
                cache[e.urn] = e;
            });
        });
        return cache;
    }, [rootNodes, rootTerms, childrenByNode]);

    const expandNode = useCallback(
        (nodeUrn: string) => {
            setExpandedNodes((prev) => {
                if (prev.has(nodeUrn)) return prev;
                const next = new Set(prev);
                next.add(nodeUrn);
                return next;
            });
            // If we've already fetched (or are fetching) children for this node, the cache is the
            // source of truth — no need to refetch on every expand toggle.
            if (childrenByNode[nodeUrn] || fetchingNodes.has(nodeUrn)) return;

            setFetchingNodes((prev) => {
                const next = new Set(prev);
                next.add(nodeUrn);
                return next;
            });

            // The chain for this node's *children* is [this node, ...this node's chain]
            // (direct-parent first). Skip the prepend if the parent entity isn't in our cache —
            // shouldn't happen in practice (we only expand nodes we've already rendered) but the
            // guard keeps the types honest.
            const parentChainForNode = chainByNode[nodeUrn] ?? [];
            const parentEntity = entityCache[nodeUrn] as GlossaryNode | undefined;
            const childChain: GlossaryNode[] = parentEntity
                ? [parentEntity, ...parentChainForNode]
                : parentChainForNode;

            apolloClient
                .query<ScrollAcrossEntitiesQuery, ScrollAcrossEntitiesQueryVariables>({
                    query: ScrollAcrossEntitiesDocument,
                    variables: getGlossaryChildrenScrollInput(nodeUrn, null, DEFAULT_GLOSSARY_CHILDREN_COUNT),
                })
                .then(({ data }) => {
                    const children = (data.scrollAcrossEntities?.searchResults ?? []).map((r) => r.entity);
                    // Synthesize the parent chain on each child entity so `useTermTreeOptions`
                    // groups them correctly and child-nodes can be expanded recursively.
                    const childrenWithChain = children.map((c) => withParentChain(c, childChain));
                    setChildrenByNode((prev) => ({ ...prev, [nodeUrn]: childrenWithChain }));
                    // Record the chain for each child-node so its own children get the right ancestry.
                    setChainByNode((prev) => {
                        const next = { ...prev };
                        childrenWithChain.forEach((c) => {
                            if (c.type === EntityType.GlossaryNode) next[c.urn] = childChain;
                        });
                        return next;
                    });
                })
                .finally(() => {
                    setFetchingNodes((prev) => {
                        const next = new Set(prev);
                        next.delete(nodeUrn);
                        return next;
                    });
                });
        },
        [apolloClient, childrenByNode, chainByNode, fetchingNodes, entityCache],
    );

    const collapseNode = useCallback((nodeUrn: string) => {
        setExpandedNodes((prev) => {
            if (!prev.has(nodeUrn)) return prev;
            const next = new Set(prev);
            next.delete(nodeUrn);
            return next;
        });
    }, []);

    const entities = useMemo<Entity[]>(() => {
        const out: Entity[] = [];
        rootNodes.forEach((n) => out.push(n));
        rootTerms.forEach((t) => out.push(t));
        expandedNodes.forEach((urn) => {
            const cached = childrenByNode[urn];
            if (cached) out.push(...cached);
        });
        return out;
    }, [rootNodes, rootTerms, expandedNodes, childrenByNode]);

    return {
        entities,
        entityCache,
        expandedNodes,
        fetchingNodes,
        expandNode,
        collapseNode,
        isLoading: rootNodesLoading || rootTermsLoading,
    };
}
