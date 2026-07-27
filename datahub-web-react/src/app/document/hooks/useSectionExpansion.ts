import { useCallback, useState } from 'react';

import { DocumentTreeNode, useDocumentTree } from '@app/document/DocumentTreeContext';
import {
    collectExpandableUrns,
    expandAllFolders,
    hasExpandedDescendant,
} from '@app/document/utils/documentTreeExpansion';

interface SectionExpansion {
    /** True when any folder within the section is currently expanded. */
    isSectionExpanded: (roots: DocumentTreeNode[]) => boolean;
    /** True while a section's expand-all traversal is in flight (drives disabled state). */
    isSectionExpanding: (sectionId: string) => boolean;
    /**
     * Expand every folder in the section if none are open, otherwise collapse them
     * all. `sectionId` is any stable key (a platform urn, or a sentinel for the
     * native group) used only to track the in-flight/disabled state.
     */
    toggleSectionExpandAll: (sectionId: string, roots: DocumentTreeNode[]) => Promise<void>;
}

/**
 * Section-scoped expand-all / collapse-all for the document sidebar tree.
 *
 * Each tree "section" (the DataHub group and each per-platform group) exposes a
 * bulk toggle. This hook owns the async expand-all traversal, the collapse-all
 * reset, and a per-section "in flight" set that drives the toggle's disabled
 * state, keeping that orchestration out of the already-large `DocumentTree`.
 *
 * `loadChildren` is passed in (rather than pulled from `useLoadDocumentTree`
 * here) so we don't spin up a second root-loading instance — the tree owns the
 * single loader and hands us just the child-fetch it already has.
 */
export function useSectionExpansion(loadChildren: (urn: string) => Promise<DocumentTreeNode[]>): SectionExpansion {
    const { expandedUrns, setExpandedUrns } = useDocumentTree();
    const [expandingSectionIds, setExpandingSectionIds] = useState<Set<string>>(new Set());

    const isSectionExpanded = useCallback(
        (roots: DocumentTreeNode[]) => hasExpandedDescendant(roots, expandedUrns),
        [expandedUrns],
    );

    const isSectionExpanding = useCallback(
        (sectionId: string) => expandingSectionIds.has(sectionId),
        [expandingSectionIds],
    );

    const toggleSectionExpandAll = useCallback(
        async (sectionId: string, roots: DocumentTreeNode[]) => {
            if (hasExpandedDescendant(roots, expandedUrns)) {
                const urns = collectExpandableUrns(roots);
                setExpandedUrns((prev) => {
                    const next = new Set(prev);
                    urns.forEach((urn) => next.delete(urn));
                    return next;
                });
                return;
            }

            setExpandingSectionIds((prev) => new Set(prev).add(sectionId));
            try {
                await expandAllFolders({
                    roots,
                    loadChildren,
                    onExpandLevel: (urns) =>
                        setExpandedUrns((prev) => {
                            const next = new Set(prev);
                            urns.forEach((urn) => next.add(urn));
                            return next;
                        }),
                });
            } finally {
                setExpandingSectionIds((prev) => {
                    const next = new Set(prev);
                    next.delete(sectionId);
                    return next;
                });
            }
        },
        [expandedUrns, setExpandedUrns, loadChildren],
    );

    return { isSectionExpanded, isSectionExpanding, toggleSectionExpandAll };
}
