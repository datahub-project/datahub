import { DocumentTreeNode } from '@app/document/DocumentTreeContext';

import { Document, DocumentState, EntityType } from '@types';

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

/**
 * Allowed entity types for document related assets.
 * Single source of truth matching backend RelatedAsset.pdl.
 * Keys are lowercase URN entity type strings (for @mention validation).
 * Values are EntityType enum values (for the dropdown picker).
 *
 * Note: Document is handled separately via relatedDocuments and is not in this map.
 * Note: structuredProperty is in the PDL but excluded because the GraphQL
 *   entityPreview fragment doesn't fetch definition.displayName, causing raw URN display.
 */
export const ALLOWED_RELATED_ASSET_TYPES: Record<string, EntityType> = {
    container: EntityType.Container,
    dataset: EntityType.Dataset,
    datajob: EntityType.DataJob,
    dataflow: EntityType.DataFlow,
    dashboard: EntityType.Dashboard,
    chart: EntityType.Chart,
    application: EntityType.Application,
    mlmodel: EntityType.Mlmodel,
    mlmodelgroup: EntityType.MlmodelGroup,
    mlprimarykey: EntityType.MlprimaryKey,
    mlfeature: EntityType.Mlfeature,
    mlfeaturetable: EntityType.MlfeatureTable,
    dataproduct: EntityType.DataProduct,
    domain: EntityType.Domain,
    glossaryterm: EntityType.GlossaryTerm,
    glossarynode: EntityType.GlossaryNode,
    tag: EntityType.Tag,
};

/**
 * Checks if a string has balanced parentheses.
 * Used to validate URNs before processing, as malformed URNs
 * with unbalanced parentheses can cause backend errors.
 *
 * @param str - The string to check
 * @returns true if parentheses are balanced, false otherwise
 */
export function hasBalancedParens(str: string): boolean {
    let count = 0;
    for (let i = 0; i < str.length; i++) {
        const char = str[i];
        if (char === '(') count += 1;
        else if (char === ')') count -= 1;
        if (count < 0) return false;
    }
    return count === 0;
}

/**
 * Checks if a URN is valid for use as a related asset.
 * Validates that:
 * 1. The URN has balanced parentheses
 * 2. The entity type is in the allowed list (ALLOWED_RELATED_ASSET_TYPES)
 *
 * @param urn - The URN to validate
 * @returns true if the URN can be used as a related asset
 */
export function isAllowedRelatedAssetUrn(urn: string): boolean {
    if (!hasBalancedParens(urn)) {
        return false;
    }

    // Extract entity type from URN (format: urn:li:entityType:...)
    const parts = urn.split(':');
    if (parts.length < 3) {
        return false;
    }

    const entityType = parts[2].toLowerCase();
    return entityType in ALLOWED_RELATED_ASSET_TYPES;
}

/**
 * Extracts a URN from a markdown link, properly handling nested parentheses.
 * Markdown links have the format: [text](url)
 * URNs can have nested parens like: urn:li:dataJob:(urn:li:dataFlow:(airflow,dag,PROD),task)
 *
 * This function finds the balanced closing paren for the markdown link,
 * correctly handling any level of nesting within the URN.
 *
 * @param content - The full markdown content
 * @param startIndex - The index of the opening '(' of the markdown link
 * @returns The extracted URN, or null if not a valid URN or unbalanced parens
 */
function extractUrnFromMarkdownLink(content: string, startIndex: number): string | null {
    let parenCount = 1;
    let i = startIndex + 1; // Start after the opening '('

    while (i < content.length && parenCount > 0) {
        if (content[i] === '(') {
            parenCount++;
        } else if (content[i] === ')') {
            parenCount--;
        }
        i++;
    }

    if (parenCount !== 0) {
        return null; // Unbalanced parentheses
    }

    // Extract the content between the parens (excluding the final closing paren)
    const urn = content.slice(startIndex + 1, i - 1);

    // Validate it's a URN
    if (!urn.startsWith('urn:li:')) {
        return null;
    }

    return urn;
}

/**
 * Extracts all URNs from markdown content that are in link format.
 * Handles URNs with deeply nested parentheses (e.g., DataJob URNs).
 *
 * Pattern: [display text](urn:li:entityType:...)
 *
 * @param content - The markdown content to search
 * @returns Array of extracted URNs
 */
export function extractUrnsFromMarkdown(content: string): string[] {
    const urns: string[] = [];

    // Find all markdown links: [text](
    const linkPattern = /\[[^\]]+\]\(/g;
    let match: RegExpExecArray | null;

    // eslint-disable-next-line no-cond-assign
    while ((match = linkPattern.exec(content)) !== null) {
        const openParenIndex = match.index + match[0].length - 1;
        const urn = extractUrnFromMarkdownLink(content, openParenIndex);

        if (urn && !urns.includes(urn)) {
            urns.push(urn);
        }
    }

    return urns;
}
