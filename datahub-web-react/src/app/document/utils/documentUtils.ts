import { DocumentTreeNode } from '@app/document/DocumentTreeContext';

import { Document } from '@types';

/**
 * Converts a Document to a DocumentTreeNode.
 *
 * @param doc - The document to convert
 * @param hasChildren - Whether this document has children
 * @returns A DocumentTreeNode representation of the document
 */
export function documentToTreeNode(doc: Document, hasChildren: boolean): DocumentTreeNode {
    return {
        urn: doc.urn,
        title: doc.info?.title || 'Untitled',
        parentUrn: doc.info?.parentDocument?.document?.urn || null,
        hasChildren,
        children: undefined, // Not loaded yet
    };
}

/**
 * Sorts documents by creation time in descending order (most recent first).
 *
 * @param documents - Array of documents to sort
 * @returns A new sorted array (does not mutate the original)
 */
export function sortDocumentsByCreationTime(documents: Document[]): Document[] {
    return [...documents].sort((a, b) => {
        const timeA = a.info?.created?.time || 0;
        const timeB = b.info?.created?.time || 0;
        return timeB - timeA; // DESC order
    });
}
