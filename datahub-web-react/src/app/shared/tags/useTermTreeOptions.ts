import { useMemo } from 'react';

import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { SelectOption } from '@src/alchemy-components/components/Select/types';

import { Entity, EntityType, GlossaryNode, GlossaryTerm } from '@types';

export interface TermTreeOption extends SelectOption {
    entity?: Entity;
    depth?: number;
    isNode?: boolean;
    color?: string;
    /** URNs of any parent nodes (root → direct-parent order). Used for expand/collapse filtering. */
    ancestorUrns?: string[];
    /** When true, this node row currently has no fetched children — useful for showing an
     * inline-loading placeholder while a `useGlossaryTreeEntities` expand is in flight. */
    isEmptyNode?: boolean;
    /** When true, this is a synthetic placeholder row (no real entity) showing that the parent
     * node's children are being fetched. Rendered as an inline loader; never selectable. */
    isLoadingPlaceholder?: boolean;
}

interface UseTermTreeOptionsArgs {
    /** Glossary-term and/or glossary-node entities to build the tree from. Non-glossary entities
     * are dropped. Terms are nested under their `parentNodes` chain; standalone nodes appear as
     * collapsible headers. */
    entities: Entity[];
    /** URNs to drop from the tree (already-applied to the resource(s), so the modal only adds). */
    excludeUrns?: string[];
    /** When provided, descendant rows are hidden unless every ancestor URN in their chain is in
     * this set. Pass `undefined` (or all node URNs) for "everything visible" — used by the search
     * path which flattens the tree. */
    expandedNodes?: Set<string>;
    /** URNs of expanded nodes whose child fetch is still in flight. Each one gets an inline
     * loading-placeholder row emitted directly under it. */
    loadingNodeUrns?: Set<string>;
}

interface UseTermTreeOptionsResult {
    /** All built rows (node headers + term rows), pre-expand filtering. */
    allOptions: TermTreeOption[];
    /** Rows after applying expand filtering. Feed this to SimpleSelect's `options`. */
    visibleOptions: TermTreeOption[];
    /** Node URNs that should render the SimpleSelect's `disabledValues` — headers aren't selectable. */
    disabledValues: string[];
    /** Node URNs with at least one descendant row visible in `allOptions`. Combined with each
     * node's `childrenCount` from the GraphQL fragment, this is what the caller uses to decide
     * whether to render the expand/collapse caret. */
    nodesWithChildren: Set<string>;
}

type GlossaryEntity = GlossaryTerm | GlossaryNode;

// Shared empty set so `loadingNodeUrns` defaults don't allocate per render.
const EMPTY_URN_SET: ReadonlySet<string> = new Set();

/** Extract a glossary entity's parent chain (direct-parent-first, per GraphQL convention). */
function getParentChain(entity: GlossaryEntity): GlossaryNode[] {
    return (entity.parentNodes?.nodes || []) as GlossaryNode[];
}

/** Compute the "root URN" used to group entities into top-level subtrees while preserving order.
 * For terms / non-root nodes, it's the URN of the topmost ancestor (the last item in the
 * direct-parent-first list). For a root entity (no parents), it's the entity's own URN — so an
 * unexpanded root node anchors its own group at its natural position. */
function getRootUrn(entity: GlossaryEntity): string {
    const chain = getParentChain(entity);
    return chain.length > 0 ? chain[chain.length - 1].urn : entity.urn;
}

/**
 * Group glossary entities into top-level subtrees keyed by their root URN.
 *
 * Filters out non-glossary entities and any URN in `excludeSet` in one pass. The `rootOrder`
 * array preserves first-appearance order so that expanding a node never reshuffles roots — the
 * root anchors its group at the position the caller passed it in.
 */
export function groupGlossaryEntitiesByRoot(
    entities: Entity[],
    excludeSet: ReadonlySet<string>,
): { groupsByRoot: Map<string, GlossaryEntity[]>; rootOrder: string[] } {
    const groupsByRoot = new Map<string, GlossaryEntity[]>();
    const rootOrder: string[] = [];
    entities.forEach((entity) => {
        if (excludeSet.has(entity.urn)) return;
        if (entity.type !== EntityType.GlossaryTerm && entity.type !== EntityType.GlossaryNode) return;
        const e = entity as GlossaryEntity;
        const rootUrn = getRootUrn(e);
        const bucket = groupsByRoot.get(rootUrn);
        if (bucket) {
            bucket.push(e);
        } else {
            groupsByRoot.set(rootUrn, [e]);
            rootOrder.push(rootUrn);
        }
    });
    return { groupsByRoot, rootOrder };
}

/**
 * Partition entities in a single root group by their direct-parent relationship within the group.
 *
 * The `childMap` lets the DFS walk emit each parent immediately followed by its children. Any
 * entity whose direct parent isn't in the group is a "subgroup root" — either a true tree root or
 * a search-path term whose ancestors weren't loaded as separate entities. We start the DFS from
 * those.
 */
export function partitionGroupByDirectParent(groupEntities: GlossaryEntity[]): {
    childMap: Map<string, GlossaryEntity[]>;
    subgroupRoots: GlossaryEntity[];
} {
    const groupUrnSet = new Set(groupEntities.map((e) => e.urn));
    const childMap = new Map<string, GlossaryEntity[]>();
    const subgroupRoots: GlossaryEntity[] = [];
    groupEntities.forEach((e) => {
        const chain = getParentChain(e);
        const directParentUrn = chain.length > 0 ? chain[0].urn : null;
        if (directParentUrn === null || !groupUrnSet.has(directParentUrn)) {
            subgroupRoots.push(e);
        } else {
            const siblings = childMap.get(directParentUrn) || [];
            siblings.push(e);
            childMap.set(directParentUrn, siblings);
        }
    });
    return { childMap, subgroupRoots };
}

interface BuildTermTreeOptionsArgs {
    entities: Entity[];
    excludeSet: ReadonlySet<string>;
    loadingSet: ReadonlySet<string>;
    /** Display name resolver. The hook wires this through `entityRegistry.getDisplayName`;
     * tests can inject a deterministic stub. */
    getDisplayName: (entity: GlossaryEntity) => string;
    /** Palette-derived color for a URN, used when an entity has no `displayProperties.colorHex`. */
    getFallbackColor: (urn: string) => string;
}

/**
 * Pure, hook-free builder for the flat tree-ordered options list. Exported so it can be
 * exercised in isolation — the hook itself is just memoization wiring on top.
 *
 * Each row rendered:
 *   - Every unique node in any ancestor chain emits an indented header row (`isNode: true`).
 *   - Standalone node entities not already emitted as ancestors appear at their natural depth.
 *   - Term rows render one depth-level below their direct parent.
 *   - For each node currently in `loadingSet` that hasn't received children yet, a synthetic
 *     `isLoadingPlaceholder` row is inserted right under the node's header.
 *
 * Output order is depth-first within each root group: a parent and its descendants are emitted
 * contiguously, before any sibling of the parent. The root order from `entities` is preserved.
 */
export function buildTermTreeOptions({
    entities,
    excludeSet,
    loadingSet,
    getDisplayName,
    getFallbackColor,
}: BuildTermTreeOptionsArgs): TermTreeOption[] {
    const result: TermTreeOption[] = [];
    const seenNodeUrns = new Set<string>();

    // Group entities by their root URN, preserving the order in which each root first appears.
    // This is what guarantees that expanding a node doesn't promote it to the top of the list —
    // the root anchors its group at its natural position regardless of how many children later
    // pile in under it.
    const { groupsByRoot, rootOrder } = groupGlossaryEntitiesByRoot(entities, excludeSet);

    // Emit a chain of ancestor headers (root → leaf), skipping any already-seen URNs so a
    // shared ancestor only appears once.
    //
    // Every node in the chain inherits the *root* node's color, even if a descendant has its own
    // `displayProperties.colorHex`. This mirrors the glossary sidebar (`NodeItem` propagates
    // `iconColor` down through recursion), where a child of "Adoption" reads the same pink
    // bookmark icon as Adoption itself rather than its own URN-hashed color. Term rows already
    // pick up the root color via `topNode` below — this brings node headers in line.
    const emitNodeChain = (chain: GlossaryNode[]) => {
        if (chain.length === 0) return;
        const rootNode = chain[0];
        const subtreeColor = rootNode.displayProperties?.colorHex || getFallbackColor(rootNode.urn);
        chain.forEach((node, depth) => {
            if (seenNodeUrns.has(node.urn)) return;
            seenNodeUrns.add(node.urn);
            result.push({
                value: node.urn,
                label: getDisplayName(node),
                isNode: true,
                depth,
                color: subtreeColor,
                ancestorUrns: chain.slice(0, depth).map((n) => n.urn),
            });
        });
    };

    rootOrder.forEach((rootUrn) => {
        const groupEntities = groupsByRoot.get(rootUrn) || [];
        const { childMap, subgroupRoots } = partitionGroupByDirectParent(groupEntities);

        // Insert a synthetic loading-placeholder row right under a node whose child fetch is
        // still in flight (and whose children haven't arrived yet — once they're in `childMap`
        // the DFS walk will emit them directly and the loader is no longer needed). Placed
        // inside the same DFS walk so the loader sits visually where the children are about
        // to appear, rather than at the top of the dropdown.
        const maybeEmitLoadingPlaceholder = (nodeUrn: string, parentDepth: number, ancestorChain: string[]) => {
            if (!loadingSet.has(nodeUrn) || childMap.has(nodeUrn)) return;
            result.push({
                value: `${nodeUrn}::loading`,
                label: 'Loading',
                isLoadingPlaceholder: true,
                depth: parentDepth + 1,
                ancestorUrns: [...ancestorChain, nodeUrn],
            });
        };

        const emitEntity = (entity: GlossaryEntity) => {
            const reversedParents = [...getParentChain(entity)].reverse(); // root → direct-parent
            if (entity.type === EntityType.GlossaryTerm) {
                const term = entity as GlossaryTerm;
                emitNodeChain(reversedParents);
                const termDepth = reversedParents.length;
                const topNode = reversedParents[0];
                const termColor = topNode
                    ? topNode.displayProperties?.colorHex || getFallbackColor(topNode.urn)
                    : getFallbackColor(term.urn);
                result.push({
                    value: term.urn,
                    label: getDisplayName(term),
                    entity: term,
                    depth: termDepth,
                    color: termColor,
                    ancestorUrns: reversedParents.map((n) => n.urn),
                });
            } else {
                const node = entity as GlossaryNode;
                const fullChain = [...reversedParents, node];
                emitNodeChain(fullChain);
                // Mark as "empty" if children haven't been fetched yet but the node *might*
                // have some — the caret stays visible so the browse flow can trigger a fetch.
                // If `childrenCount` is present and reports zero on both axes, it's a true
                // leaf node (e.g. an empty term group) and we suppress the caret to match the
                // glossary sidebar. When `childrenCount` is missing, we conservatively show
                // the caret since we can't prove it's empty.
                const count = node.childrenCount;
                const knownEmpty = count != null && (count.termsCount ?? 0) === 0 && (count.nodesCount ?? 0) === 0;
                if (!childMap.has(node.urn) && !knownEmpty) {
                    const lastOption = result[result.length - 1];
                    if (lastOption && lastOption.value === node.urn) {
                        lastOption.isEmptyNode = true;
                    }
                }
                maybeEmitLoadingPlaceholder(
                    node.urn,
                    fullChain.length - 1,
                    reversedParents.map((n) => n.urn),
                );
            }
            // DFS: emit children right after their parent, before any siblings of the parent.
            (childMap.get(entity.urn) || []).forEach(emitEntity);
        };

        subgroupRoots.forEach(emitEntity);
    });

    return result;
}

/**
 * Hook wrapper around {@link buildTermTreeOptions} that memoizes derived state for `SimpleSelect`.
 *
 * The pure builder is exported separately for unit testing; this hook just supplies the entity
 * registry and color palette and derives `visibleOptions` / `disabledValues` / `nodesWithChildren`
 * from the flat options list.
 */
export function useTermTreeOptions({
    entities,
    excludeUrns,
    expandedNodes,
    loadingNodeUrns,
}: UseTermTreeOptionsArgs): UseTermTreeOptionsResult {
    const entityRegistry = useEntityRegistry();
    const generateColor = useGenerateGlossaryColorFromPalette();

    const excludeSet = useMemo(() => new Set(excludeUrns || []), [excludeUrns]);
    const loadingSet = loadingNodeUrns ?? EMPTY_URN_SET;

    const allOptions = useMemo<TermTreeOption[]>(
        () =>
            buildTermTreeOptions({
                entities,
                excludeSet,
                loadingSet,
                getDisplayName: (entity) => entityRegistry.getDisplayName(entity.type, entity),
                getFallbackColor: generateColor,
            }),
        [entities, excludeSet, loadingSet, entityRegistry, generateColor],
    );

    const nodesWithChildren = useMemo(() => {
        const withChildren = new Set<string>();
        allOptions.forEach((opt) => {
            (opt.ancestorUrns || []).forEach((urn) => withChildren.add(urn));
        });
        return withChildren;
    }, [allOptions]);

    const visibleOptions = useMemo(() => {
        if (!expandedNodes) return allOptions;
        return allOptions.filter((opt) => {
            const ancestors = opt.ancestorUrns || [];
            return ancestors.every((urn) => expandedNodes.has(urn));
        });
    }, [allOptions, expandedNodes]);

    // Headers and loading placeholders should never be selectable.
    const disabledValues = useMemo(
        () => allOptions.filter((o) => o.isNode || o.isLoadingPlaceholder).map((o) => o.value),
        [allOptions],
    );

    return {
        allOptions,
        visibleOptions,
        disabledValues,
        nodesWithChildren,
    };
}
