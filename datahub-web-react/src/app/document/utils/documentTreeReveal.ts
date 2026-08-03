import { DocumentTreeNode } from '@app/document/DocumentTreeContext';

/**
 * Minimal parent-document shape from `getDocument` / search (`parentDocuments.documents`).
 * API order is direct-parent first; callers reverse to walk root → leaf.
 */
export interface DocumentParentRef {
    urn: string;
    title?: string | null;
}

export interface RevealDocumentInTreeArgs {
    documentUrn: string;
    documentTitle?: string | null;
    /** Parent chain in API order: [direct parent, grandparent, ...]. */
    parentDocuments: DocumentParentRef[];
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
    getNode,
    expandNode,
    ensureNode,
    loadChildren,
    loadMoreChildren,
    hasMoreChildren,
}: RevealDocumentInTreeArgs): Promise<void> {
    const ancestors = [...parentDocuments].reverse();

    if (ancestors.length === 0) {
        ensureNode({
            urn: documentUrn,
            title: untitledTitle(documentTitle),
            parentUrn: null,
            hasChildren: getNode(documentUrn)?.hasChildren ?? false,
        });
        return;
    }

    for (let i = 0; i < ancestors.length; i++) {
        const ancestor = ancestors[i];
        const parentUrn = i === 0 ? null : ancestors[i - 1].urn;
        const nextUrn = i < ancestors.length - 1 ? ancestors[i + 1].urn : documentUrn;

        ensureNode({
            urn: ancestor.urn,
            title: untitledTitle(ancestor.title),
            parentUrn,
            hasChildren: true,
        });

        expandNode(ancestor.urn);

        // eslint-disable-next-line no-await-in-loop
        let children = await loadChildren(ancestor.urn);

        while (!children.some((child) => child.urn === nextUrn) && hasMoreChildren(ancestor.urn)) {
            // eslint-disable-next-line no-await-in-loop
            const more = await loadMoreChildren(ancestor.urn);
            children = children.concat(more);
        }

        if (!getNode(nextUrn) && nextUrn !== documentUrn) {
            ensureNode({
                urn: nextUrn,
                title: untitledTitle(ancestors[i + 1]?.title),
                parentUrn: ancestor.urn,
                hasChildren: true,
            });
        }
    }

    const directParentUrn = ancestors[ancestors.length - 1].urn;
    ensureNode({
        urn: documentUrn,
        title: untitledTitle(documentTitle),
        parentUrn: directParentUrn,
        hasChildren: getNode(documentUrn)?.hasChildren ?? false,
    });
}
