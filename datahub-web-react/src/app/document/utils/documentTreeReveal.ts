import { DocumentTreeNode } from '@app/document/DocumentTreeContext';

import { DataPlatform } from '@types';

/**
 * Minimal parent-document shape from `getDocument` / search (`parentDocuments.documents`).
 * API order is direct-parent first; callers reverse to walk root → leaf.
 */
export interface DocumentParentRef {
    urn: string;
    title?: string | null;
}

/** Source fields needed so reveal stubs land in the right sidebar section. */
export type RevealDocumentNodeMeta = {
    platform?: DataPlatform | null;
    isExternal?: boolean;
    isUnpublished?: boolean;
    lastModifiedAt?: number;
};

export interface RevealDocumentInTreeArgs {
    documentUrn: string;
    documentTitle?: string | null;
    /** Parent chain in API order: [direct parent, grandparent, ...]. */
    parentDocuments: DocumentParentRef[];
    /**
     * Platform / external / timestamps from getDocument. Applied to reveal stubs so
     * e.g. a Notion root isn't classified as native DataHub when it wasn't already
     * on the loaded root page.
     */
    documentMeta?: RevealDocumentNodeMeta;
    getNode: (urn: string) => DocumentTreeNode | undefined;
    expandNode: (urn: string) => void;
    /** Synchronously commit the node into tree state before subsequent loads. */
    ensureNode: (node: DocumentTreeNode) => void;
    loadChildren: (parentUrn: string) => Promise<DocumentTreeNode[]>;
    loadMoreChildren: (parentUrn: string) => Promise<DocumentTreeNode[]>;
    hasMoreChildren: (parentUrn: string) => boolean;
}

function untitledTitle(title?: string | null): string {
    return title?.trim() ? title : 'Untitled';
}

function stubNode({
    urn,
    title,
    parentUrn,
    hasChildren,
    documentMeta,
    includeTimestamps,
}: {
    urn: string;
    title?: string | null;
    parentUrn: string | null;
    hasChildren: boolean;
    documentMeta?: RevealDocumentNodeMeta;
    includeTimestamps?: boolean;
}): DocumentTreeNode {
    return {
        urn,
        title: untitledTitle(title),
        parentUrn,
        hasChildren,
        platform: documentMeta?.platform ?? null,
        isExternal: documentMeta?.isExternal,
        isUnpublished: includeTimestamps ? documentMeta?.isUnpublished : undefined,
        lastModifiedAt: includeTimestamps ? documentMeta?.lastModifiedAt : undefined,
    };
}

/**
 * Expand and lazy-load ancestors so `documentUrn` can mount in the sidebar tree.
 *
 * Like global search: the root list stays in server sort order from the top of
 * page 1. We never page-through or inject a missing root into the list (that
 * yanked scroll to the bottom). If the open doc's root isn't in the already-loaded
 * window, reveal is a no-op — the profile still works; the row appears when the
 * user scrolls the sorted list far enough.
 */
export async function revealDocumentInTree({
    documentUrn,
    documentTitle,
    parentDocuments,
    documentMeta,
    getNode,
    expandNode,
    ensureNode,
    loadChildren,
    loadMoreChildren,
    hasMoreChildren,
}: RevealDocumentInTreeArgs): Promise<void> {
    const ancestors = [...parentDocuments].reverse();

    // Root document: only meaningful if it's already in the loaded sorted window.
    if (ancestors.length === 0) {
        return;
    }

    // Nested: root ancestor must already be loaded — do not fetch every root page.
    const rootAncestor = ancestors[0];
    if (!getNode(rootAncestor.urn)) {
        return;
    }

    for (let i = 0; i < ancestors.length; i++) {
        const ancestor = ancestors[i];
        const parentUrn = i === 0 ? null : ancestors[i - 1].urn;
        const nextUrn = i < ancestors.length - 1 ? ancestors[i + 1].urn : documentUrn;

        if (parentUrn !== null && !getNode(ancestor.urn)) {
            ensureNode(
                stubNode({
                    urn: ancestor.urn,
                    title: ancestor.title,
                    parentUrn,
                    hasChildren: true,
                    documentMeta,
                }),
            );
        }

        expandNode(ancestor.urn);

        // eslint-disable-next-line no-await-in-loop
        let children = await loadChildren(ancestor.urn);

        while (!children.some((child) => child.urn === nextUrn) && hasMoreChildren(ancestor.urn)) {
            // eslint-disable-next-line no-await-in-loop
            const more = await loadMoreChildren(ancestor.urn);
            children = children.concat(more);
        }

        if (!getNode(nextUrn) && nextUrn !== documentUrn) {
            ensureNode(
                stubNode({
                    urn: nextUrn,
                    title: ancestors[i + 1]?.title,
                    parentUrn: ancestor.urn,
                    hasChildren: true,
                    documentMeta,
                }),
            );
        }
    }

    const directParentUrn = ancestors[ancestors.length - 1].urn;
    if (!getNode(documentUrn)) {
        ensureNode(
            stubNode({
                urn: documentUrn,
                title: documentTitle,
                parentUrn: directParentUrn,
                hasChildren: false,
                documentMeta,
                includeTimestamps: true,
            }),
        );
    }
}
