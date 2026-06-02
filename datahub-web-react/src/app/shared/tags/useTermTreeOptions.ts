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
 * Builds a tree-ordered flat list of `SelectOption`s for glossary terms so an alchemy `SimpleSelect`
 * can render them as an indented hierarchy (node header → indented term children) with native
 * multi-select checkboxes.
 *
 * Accepts both `GlossaryTerm` and `GlossaryNode` entities so the modal can show standalone node
 * headers before any of their children are fetched (the lazy-expand flow). Output order is driven
 * by the order roots first appear in `entities`: expanding a node never re-shuffles the list —
 * it just inserts the new children in-place under the existing header.
 *
 * Each rendered row:
 *   - Each unique node in any ancestor chain emits an indented header row (`isNode: true`).
 *   - Standalone node entities not already emitted as ancestors appear at their natural depth.
 *   - Term rows render one depth-level below their direct parent.
 *
 * Node URNs go in `disabledValues` so users can only check off term rows. Visibility is filtered
 * via `expandedNodes`: a row is hidden if any of its ancestors aren't in that set.
 */
export function useTermTreeOptions({
    entities,
    excludeUrns,
    expandedNodes,
}: UseTermTreeOptionsArgs): UseTermTreeOptionsResult {
    const entityRegistry = useEntityRegistry();
    const generateColor = useGenerateGlossaryColorFromPalette();

    const excludeSet = useMemo(() => new Set(excludeUrns || []), [excludeUrns]);

    const allOptions = useMemo<TermTreeOption[]>(() => {
        const result: TermTreeOption[] = [];
        const seenNodeUrns = new Set<string>();

        // Group entities by their root URN, preserving the order in which each root first appears.
        // This is what guarantees that expanding a node doesn't promote it to the top of the list —
        // the root anchors its group at its natural position regardless of how many children later
        // pile in under it.
        const groupsByRoot = new Map<string, GlossaryEntity[]>();
        const rootOrder: string[] = [];
        entities.forEach((entity) => {
            if (excludeSet.has(entity.urn)) return;
            if (entity.type !== EntityType.GlossaryTerm && entity.type !== EntityType.GlossaryNode) return;
            const e = entity as GlossaryEntity;
            const rootUrn = getRootUrn(e);
            const bucket = groupsByRoot.get(rootUrn);
            if (bucket) bucket.push(e);
            else {
                groupsByRoot.set(rootUrn, [e]);
                rootOrder.push(rootUrn);
            }
        });

        // Emit a chain of ancestor headers (root → leaf), skipping any already-seen URNs so a
        // shared ancestor only appears once.
        const emitNodeChain = (chain: GlossaryNode[]) => {
            chain.forEach((node, depth) => {
                if (seenNodeUrns.has(node.urn)) return;
                seenNodeUrns.add(node.urn);
                result.push({
                    value: node.urn,
                    label: entityRegistry.getDisplayName(node.type, node),
                    isNode: true,
                    depth,
                    color: node.displayProperties?.colorHex || generateColor(node.urn),
                    ancestorUrns: chain.slice(0, depth).map((n) => n.urn),
                });
            });
        };

        rootOrder.forEach((rootUrn) => {
            const groupEntities = groupsByRoot.get(rootUrn) || [];

            // A node is "empty" if no later entity in this group references it as an ancestor.
            // The browse flow uses this to decide whether to render an inline-loading placeholder
            // on the caret column.
            const referencedAsAncestor = new Set<string>();
            groupEntities.forEach((e) => {
                getParentChain(e).forEach((p) => referencedAsAncestor.add(p.urn));
            });

            groupEntities.forEach((entity) => {
                const reversedParents = [...getParentChain(entity)].reverse(); // root → direct-parent
                if (entity.type === EntityType.GlossaryTerm) {
                    const term = entity as GlossaryTerm;
                    emitNodeChain(reversedParents);
                    const termDepth = reversedParents.length;
                    const topNode = reversedParents[0];
                    const termColor = topNode
                        ? topNode.displayProperties?.colorHex || generateColor(topNode.urn)
                        : generateColor(term.urn);
                    result.push({
                        value: term.urn,
                        label: entityRegistry.getDisplayName(term.type, term),
                        entity: term,
                        depth: termDepth,
                        color: termColor,
                        ancestorUrns: reversedParents.map((n) => n.urn),
                    });
                } else {
                    const node = entity as GlossaryNode;
                    const fullChain = [...reversedParents, node];
                    emitNodeChain(fullChain);
                    // If this node was just emitted as a leaf row (i.e. it's still the last result)
                    // and nothing else in the group references it as an ancestor, mark it empty
                    // so the modal can render a "click to load" affordance.
                    if (!referencedAsAncestor.has(node.urn)) {
                        const lastOption = result[result.length - 1];
                        if (lastOption && lastOption.value === node.urn) {
                            lastOption.isEmptyNode = true;
                        }
                    }
                }
            });
        });

        return result;
    }, [entities, excludeSet, entityRegistry, generateColor]);

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

    const disabledValues = useMemo(() => allOptions.filter((o) => o.isNode).map((o) => o.value), [allOptions]);

    return {
        allOptions,
        visibleOptions,
        disabledValues,
        nodesWithChildren,
    };
}
