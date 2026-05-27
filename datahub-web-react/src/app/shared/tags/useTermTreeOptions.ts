import { useCallback, useMemo, useState } from 'react';

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
}

interface UseTermTreeOptionsArgs {
    /** Glossary-term entities from the current search/recommendation result. Non-term entities are dropped. */
    entities: Entity[];
    /** URNs to drop from the tree (already-applied to the resource(s), so the modal only adds). */
    excludeUrns?: string[];
    /** While true, collapse state is ignored so matching descendants stay visible during search. */
    ignoreCollapsed?: boolean;
}

interface UseTermTreeOptionsResult {
    /** All built rows (node headers + term rows), pre-collapse filtering. */
    allOptions: TermTreeOption[];
    /** Rows after applying collapsed-node filtering. Feed this to SimpleSelect's `options`. */
    visibleOptions: TermTreeOption[];
    /** Node URNs that should render the SimpleSelect's `disabledValues` — headers aren't selectable. */
    disabledValues: string[];
    /** Node URNs with at least one descendant row — only these get an expand/collapse caret. */
    nodesWithChildren: Set<string>;
    /** Set of node URNs currently in the collapsed state. */
    collapsedNodes: Set<string>;
    toggleNodeCollapsed: (nodeUrn: string) => void;
}

/**
 * Builds a tree-ordered flat list of `SelectOption`s for glossary terms so an alchemy `SimpleSelect`
 * can render them as an indented hierarchy (node header → indented term children) with native
 * multi-select checkboxes.
 *
 * For each lineage:
 *   - Emit a row per unique parent node (depth-indented, marked `isNode: true`).
 *   - Emit the term rows underneath (one extra depth level).
 *   - Already-applied terms are dropped via `excludeUrns`; node headers are only emitted when they
 *     have at least one selectable child (a node group with all-applied children is skipped).
 *
 * Node URNs go in `disabledValues` so users can only check off term rows. Headers carry an
 * `ancestorUrns` chain that the consumer uses for expand/collapse filtering.
 *
 * Lifted out of `AddTermsModal` so the modal stays focused on UI; the algorithm has a fair amount
 * of logic and tests can target this hook in isolation if we want unit coverage later.
 */
export function useTermTreeOptions({
    entities,
    excludeUrns,
    ignoreCollapsed,
}: UseTermTreeOptionsArgs): UseTermTreeOptionsResult {
    const entityRegistry = useEntityRegistry();
    const generateColor = useGenerateGlossaryColorFromPalette();

    const excludeSet = useMemo(() => new Set(excludeUrns || []), [excludeUrns]);

    const allOptions = useMemo<TermTreeOption[]>(() => {
        const result: TermTreeOption[] = [];
        const seenNodeUrns = new Set<string>();
        const groupedByLineage = new Map<string, GlossaryTerm[]>();
        const lineageOrder: string[] = [];

        entities.forEach((entity) => {
            if (entity.type !== EntityType.GlossaryTerm) return;
            if (excludeSet.has(entity.urn)) return;
            const term = entity as GlossaryTerm;
            const lineage = (term.parentNodes?.nodes || []).map((n) => n.urn).join('/');
            const existing = groupedByLineage.get(lineage);
            if (existing) {
                existing.push(term);
            } else {
                groupedByLineage.set(lineage, [term]);
                lineageOrder.push(lineage);
            }
        });

        lineageOrder.forEach((lineage) => {
            const terms = groupedByLineage.get(lineage);
            if (!terms || terms.length === 0) return;
            // ParentNodes are direct-parent-first; reverse to render root → leaf.
            const nodes = [...(terms[0].parentNodes?.nodes || [])].reverse();
            nodes.forEach((node, depth) => {
                if (seenNodeUrns.has(node.urn)) return;
                seenNodeUrns.add(node.urn);
                const nodeColor = (node as GlossaryNode).displayProperties?.colorHex || generateColor(node.urn);
                result.push({
                    value: node.urn,
                    label: entityRegistry.getDisplayName(node.type, node),
                    isNode: true,
                    depth,
                    color: nodeColor,
                    ancestorUrns: nodes.slice(0, depth).map((n) => n.urn),
                });
            });
            const termDepth = nodes.length;
            const rootNode = nodes[0];
            const termColor = rootNode
                ? (rootNode as GlossaryNode).displayProperties?.colorHex || generateColor(rootNode.urn)
                : undefined;
            const termAncestorUrns = nodes.map((n) => n.urn);
            terms.forEach((term) => {
                result.push({
                    value: term.urn,
                    label: entityRegistry.getDisplayName(term.type, term),
                    entity: term,
                    depth: termDepth,
                    color: termColor || generateColor(term.urn),
                    ancestorUrns: termAncestorUrns,
                });
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

    const [collapsedNodes, setCollapsedNodes] = useState<Set<string>>(new Set());

    const toggleNodeCollapsed = useCallback((nodeUrn: string) => {
        setCollapsedNodes((prev) => {
            const next = new Set(prev);
            if (next.has(nodeUrn)) next.delete(nodeUrn);
            else next.add(nodeUrn);
            return next;
        });
    }, []);

    const visibleOptions = useMemo(() => {
        if (ignoreCollapsed || collapsedNodes.size === 0) return allOptions;
        return allOptions.filter((opt) => {
            const ancestors = opt.ancestorUrns || [];
            return !ancestors.some((urn) => collapsedNodes.has(urn));
        });
    }, [allOptions, collapsedNodes, ignoreCollapsed]);

    const disabledValues = useMemo(() => allOptions.filter((o) => o.isNode).map((o) => o.value), [allOptions]);

    return {
        allOptions,
        visibleOptions,
        disabledValues,
        nodesWithChildren,
        collapsedNodes,
        toggleNodeCollapsed,
    };
}
