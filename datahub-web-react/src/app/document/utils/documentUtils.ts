import { DocumentTreeNode } from '@app/document/DocumentTreeContext';

import { Document, DocumentState } from '@types';

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

/**
 * Extracts related asset URNs from a document.
 * Handles documents from GraphQL queries where relatedAssets may be null or undefined.
 *
 * @param document - Document with info.relatedAssets structure
 * @returns Array of asset URNs (empty array if none found)
 */
export function extractRelatedAssetUrns(
    document: { info?: { relatedAssets?: Array<{ asset: { urn: string } }> | null } | null } | null,
): string[] {
    return document?.info?.relatedAssets?.map((relatedAsset) => relatedAsset.asset.urn) || [];
}

/**
 * Merges multiple arrays of URNs and removes duplicates.
 *
 * @param urnArrays - Variable number of URN arrays to merge
 * @returns A new array with unique URNs
 */
export function mergeUrns(...urnArrays: (string[] | undefined | null)[]): string[] {
    return [...new Set(urnArrays.flat().filter((urn): urn is string => Boolean(urn)))];
}

/**
 * Creates a default document input for creating a new document.
 *
 * @param options - Configuration options for the document
 * @param options.title - Document title (defaults to 'New Document')
 * @param options.parentUrn - Optional parent document URN
 * @param options.relatedAssetUrns - Optional array of related asset URNs
 * @param options.state - Document state (defaults to Published)
 * @param options.showInGlobalContext - Whether to show in global context (defaults to true)
 * @returns CreateDocumentInput object ready for mutation
 */
export function createDefaultDocumentInput(options?: {
    title?: string;
    parentUrn?: string | null;
    relatedAssetUrns?: string[];
    state?: DocumentState;
    showInGlobalContext?: boolean;
}) {
    return {
        title: options?.title || 'New Document',
        parentDocument: options?.parentUrn || undefined,
        relatedAssets: options?.relatedAssetUrns || undefined,
        contents: { text: '' },
        state: options?.state || DocumentState.Published,
        settings: { showInGlobalContext: options?.showInGlobalContext ?? true },
    };
}
