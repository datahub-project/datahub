import { DocumentTreeNode } from '@app/document/DocumentTreeContext';
import { DOCUMENT_SIDEBAR_SORT } from '@app/document/utils/documentSidebarSort';

/**
 * Normalize titles for Name A–Z so decorative prefixes don't dominate.
 * "[SEED] Expand" → "seed] expand" (sorts with S, not under "[").
 */
export function documentTitleSortKey(title: string): string {
    const trimmed = title.trim();
    const stripped = trimmed.replace(/^[^a-zA-Z0-9]+/, '');
    return (stripped || trimmed).toLocaleLowerCase('en');
}

/** Letters first, then numbers, then everything else — matches “Name A–Z” expectations. */
function titleSortBucket(key: string): number {
    const first = key.charAt(0);
    if (first >= 'a' && first <= 'z') return 0;
    if (first >= '0' && first <= '9') return 1;
    return 2;
}

/**
 * Human-friendly title compare for the documents sidebar.
 * Exported for unit tests.
 */
export function compareDocumentTitles(a: string, b: string): number {
    const keyA = documentTitleSortKey(a);
    const keyB = documentTitleSortKey(b);
    const bucketDiff = titleSortBucket(keyA) - titleSortBucket(keyB);
    if (bucketDiff !== 0) return bucketDiff;
    const byKey = keyA.localeCompare(keyB, 'en', { numeric: true, sensitivity: 'base' });
    if (byKey !== 0) return byKey;
    // Stable tie-break on the raw title (keeps "[SEED] A" vs "SEED A" deterministic).
    return a.localeCompare(b, 'en', { numeric: true, sensitivity: 'base' });
}

/**
 * Client-side tree ordering for the documents sidebar.
 * Name sorts use {@link compareDocumentTitles}; lastModified uses lastModifiedAt.
 */
export function sortDocumentTreeNodes(nodes: DocumentTreeNode[], sort: string): DocumentTreeNode[] {
    if (nodes.length <= 1) return nodes;

    if (sort === DOCUMENT_SIDEBAR_SORT.NAME_ASC) {
        return [...nodes].sort((a, b) => compareDocumentTitles(a.title, b.title));
    }
    if (sort === DOCUMENT_SIDEBAR_SORT.NAME_DESC) {
        return [...nodes].sort((a, b) => compareDocumentTitles(b.title, a.title));
    }
    if (sort === DOCUMENT_SIDEBAR_SORT.LAST_MODIFIED_DESC) {
        return [...nodes].sort((a, b) => (b.lastModifiedAt ?? 0) - (a.lastModifiedAt ?? 0));
    }

    return nodes;
}
