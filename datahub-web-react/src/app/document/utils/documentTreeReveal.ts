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

/** Source / sort fields needed so reveal stubs land in the right sidebar section. */
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
     * e.g. a Notion root isn't classified as native DataHub (and prepended to that
     * section) when it wasn't already on the loaded root page.
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
        // Same source tree as the open document — keeps external roots in Notion/GitHub/…
        // instead of the DataHub section when they weren't on the first root page.
        platform: documentMeta?.platform ?? null,
        isExternal: documentMeta?.isExternal,
        isUnpublished: includeTimestamps ? documentMeta?.isUnpublished : undefined,
        lastModifiedAt: includeTimestamps ? documentMeta?.lastModifiedAt : undefined,
    };
}

/**
 * Expand and lazy-load every ancestor so `documentUrn` is mounted in the sidebar tree.
 *
 * Deep links only open the profile; nested rows are not fetched until parents expand.
 * This walks the parent chain root-first, expands each folder, loads children (paging
 * until the next path node appears), and injects stubs when a node is missing from
 * the currently loaded window (e.g. root beyond the first page).
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

    if (ancestors.length === 0) {
        ensureNode(
            stubNode({
                urn: documentUrn,
                title: documentTitle,
                parentUrn: null,
                hasChildren: getNode(documentUrn)?.hasChildren ?? false,
                documentMeta,
                includeTimestamps: true,
            }),
        );
        return;
    }

    for (let i = 0; i < ancestors.length; i++) {
        const ancestor = ancestors[i];
        const parentUrn = i === 0 ? null : ancestors[i - 1].urn;
        const nextUrn = i < ancestors.length - 1 ? ancestors[i + 1].urn : documentUrn;

        ensureNode(
            stubNode({
                urn: ancestor.urn,
                title: ancestor.title,
                parentUrn,
                hasChildren: true,
                documentMeta,
            }),
        );

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
    ensureNode(
        stubNode({
            urn: documentUrn,
            title: documentTitle,
            parentUrn: directParentUrn,
            hasChildren: getNode(documentUrn)?.hasChildren ?? false,
            documentMeta,
            includeTimestamps: true,
        }),
    );
}
