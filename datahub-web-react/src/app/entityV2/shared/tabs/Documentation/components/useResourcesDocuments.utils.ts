import { Document, EntityType } from '@types';

/**
 * In-memory Resources list: hide successful unlinks, and keep newly linked
 * documents visible even when related-documents search has not indexed them yet.
 */
export function mergeResourcesDocuments({
    fetched,
    added,
    removedUrns,
}: {
    fetched: Document[];
    added: Document[];
    removedUrns: Set<string>;
}): Document[] {
    const fetchedUrns = new Set(fetched.map((document) => document.urn));
    const pendingAdded = added.filter((document) => !fetchedUrns.has(document.urn) && !removedUrns.has(document.urn));
    const visibleFetched = fetched.filter((document) => !removedUrns.has(document.urn));
    return [...pendingAdded, ...visibleFetched];
}

/** Drop local additions once the related-documents query returns them. */
export function pruneAddedDocuments(added: Document[], fetchedUrns: Set<string>): Document[] {
    if (added.length === 0 || fetchedUrns.size === 0) return added;
    const next = added.filter((document) => !fetchedUrns.has(document.urn));
    return next.length === added.length ? added : next;
}

export function upsertAddedDocuments(existing: Document[], incoming: Document[]): Document[] {
    if (incoming.length === 0) return existing;
    const byUrn = new Map(existing.map((document) => [document.urn, document]));
    incoming.forEach((document) => {
        byUrn.set(document.urn, document);
    });
    return [...byUrn.values()];
}

/** Enough of a Document for a Resources pill + later unlink after create. */
export function createPendingResourceDocument(urn: string, entityUrn: string): Document {
    return {
        urn,
        type: EntityType.Document,
        info: {
            title: 'New Document',
            relatedAssets: [{ asset: { urn: entityUrn } }],
        },
    } as Document;
}
