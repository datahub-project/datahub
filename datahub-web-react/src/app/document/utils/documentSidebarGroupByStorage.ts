import {
    DOCUMENT_GROUP_BY,
    DocumentGroupByValue,
    isDocumentGroupByValue,
} from '@app/document/utils/documentSidebarGrouping';

export const DOCUMENT_SIDEBAR_GROUP_BY_STORAGE_KEY = 'documentsSidebarGroupBy';

export const DEFAULT_DOCUMENT_SIDEBAR_GROUP_BY: DocumentGroupByValue = DOCUMENT_GROUP_BY.SOURCE;

export function readStoredDocumentGroupBy(): DocumentGroupByValue {
    try {
        const raw = localStorage.getItem(DOCUMENT_SIDEBAR_GROUP_BY_STORAGE_KEY);
        if (raw && isDocumentGroupByValue(raw)) return raw;
    } catch {
        // Safari private mode / blocked storage — fall through to default.
    }
    return DEFAULT_DOCUMENT_SIDEBAR_GROUP_BY;
}

export function writeStoredDocumentGroupBy(value: DocumentGroupByValue): void {
    try {
        localStorage.setItem(DOCUMENT_SIDEBAR_GROUP_BY_STORAGE_KEY, value);
    } catch {
        // Ignore quota / private-mode failures; in-memory state still works for the session.
    }
}
