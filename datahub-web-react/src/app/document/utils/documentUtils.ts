import { FileDashed } from '@phosphor-icons/react/dist/csr/FileDashed';
import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { Folder } from '@phosphor-icons/react/dist/csr/Folder';
import { FolderDashed } from '@phosphor-icons/react/dist/csr/FolderDashed';
import i18next from 'i18next';

import { DocumentCreator, DocumentTreeNode } from '@app/document/DocumentTreeContext';

import { Document, DocumentSourceType, DocumentState, EntityType } from '@types';

type DocumentWithUnpublishedSignal =
    | {
          info?: { status?: { state?: DocumentState | null } | null } | null;
      }
    | null
    | undefined;

/**
 * Returns true when a document should render as "not-yet-published" — i.e. anything
 * other than PUBLISHED. Used to drive the dashed sidebar icon.
 *
 * @param doc - Document (or partial document carrying status fields)
 * @returns true when the doc should be rendered as unpublished
 */
export function isDocumentUnpublished(doc: DocumentWithUnpublishedSignal): boolean {
    return doc?.info?.status?.state === DocumentState.Unpublished;
}

/**
 * Returns true when a document is sourced from an external platform (e.g. Notion, GitHub),
 * as opposed to a NATIVE document authored directly in DataHub. Documents with no
 * `info.source.sourceType` populated default to false (treated as native).
 *
 * @param doc - Document (or partial document carrying source fields)
 * @returns true when the doc is sourced from an external platform
 */
export function isExternalDocument(
    doc: { info?: { source?: { sourceType?: DocumentSourceType | null } | null } | null } | null | undefined,
): boolean {
    return doc?.info?.source?.sourceType === DocumentSourceType.External;
}

/**
 * Resolves the Phosphor icon component for a document row given its branch/published state.
 * Consumed by the Documents sidebar tree and by any flat list of documents (e.g. Resources)
 * that wants icon parity with the sidebar. Callers rendering a flat list pass
 * `hasChildren: false` — the "folder" variants only apply inside the tree.
 */
export function pickTreeIcon({ hasChildren, isUnpublished }: { hasChildren: boolean; isUnpublished: boolean }) {
    if (hasChildren) return isUnpublished ? FolderDashed : Folder;
    return isUnpublished ? FileDashed : FileText;
}

/**
 * Minimal shape of the actor returned by `documentSidebarFields.info.created.actor`.
 * Loose by design — different actor entity types expose displayName via different
 * field paths, and this util needs to gracefully read whichever is populated.
 */
type ActorDisplayShape = {
    urn?: string | null;
    username?: string | null;
    name?: string | null;
    editableProperties?: { displayName?: string | null } | null;
    properties?: {
        displayName?: string | null;
        fullName?: string | null;
        firstName?: string | null;
        lastName?: string | null;
    } | null;
    info?: {
        displayName?: string | null;
        fullName?: string | null;
    } | null;
};

/**
 * Resolves a user-facing display name from an actor entity. Mirrors the precedence
 * used by the entity registry's CorpUser/CorpGroup displayName resolvers without
 * pulling the registry into this pure utility — `documentToTreeNode` runs outside
 * a React context.
 *
 * Precedence (first non-empty wins):
 *   1. editableProperties.displayName (user-supplied override)
 *   2. properties.displayName / info.displayName (data-source canonical name)
 *   3. properties.fullName / info.fullName
 *   4. properties.firstName + properties.lastName (joined)
 *   5. username (CorpUser fallback)
 *   6. name (CorpGroup fallback)
 *   7. urn (last-ditch fallback so the row is never blank)
 *
 * @param actor - Actor entity (or partial)
 * @returns Display name string, never empty
 */
export function resolveActorDisplayName(actor: ActorDisplayShape | null | undefined): string {
    if (!actor) return '';
    const { editableProperties, properties, info, username, name, urn } = actor;
    const firstLast = [properties?.firstName, properties?.lastName].filter(Boolean).join(' ').trim();
    return (
        editableProperties?.displayName ||
        properties?.displayName ||
        info?.displayName ||
        properties?.fullName ||
        info?.fullName ||
        firstLast ||
        username ||
        name ||
        urn ||
        ''
    );
}

/**
 * Extracts the {@link DocumentCreator} from a document's `info.created.actor`,
 * returning `null` when no actor info is present. The result is what the sidebar's
 * Author multi-select uses to dedupe and render owner pills.
 *
 * @param doc - Document (or partial document carrying creator fields)
 * @returns Resolved creator, or null when the actor is missing
 */
export function extractDocumentCreator(
    doc:
        | {
              info?: {
                  created?: {
                      actor?:
                          | (ActorDisplayShape & {
                                type?: EntityType | null;
                                editableProperties?: {
                                    displayName?: string | null;
                                    pictureLink?: string | null;
                                } | null;
                            })
                          | null;
                  } | null;
              } | null;
          }
        | null
        | undefined,
): DocumentCreator | null {
    const actor = doc?.info?.created?.actor;
    if (!actor?.urn || !actor?.type) return null;
    return {
        urn: actor.urn,
        type: actor.type,
        displayName: resolveActorDisplayName(actor),
        pictureLink: actor.editableProperties?.pictureLink ?? null,
    };
}

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
        title: doc.info?.title || i18next.t('entity.types:document.untitledFallback'),
        parentUrn: doc.info?.parentDocument?.document?.urn || null,
        hasChildren,
        children: undefined, // Not loaded yet
        isUnpublished: isDocumentUnpublished(doc),
        isExternal: isExternalDocument(doc),
        platform: doc.platform ?? null,
        creator: extractDocumentCreator(doc),
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
 * Extracts related document URNs from a document (doc-to-doc links).
 * Mirrors {@link extractRelatedAssetUrns} for the `relatedDocuments` list.
 *
 * @param document - Document with info.relatedDocuments structure
 * @returns Array of document URNs (empty array if none found)
 */
export function extractRelatedDocumentUrns(
    document: { info?: { relatedDocuments?: Array<{ document?: { urn: string } | null }> | null } | null } | null,
): string[] {
    return (
        document?.info?.relatedDocuments
            ?.map((related) => related?.document?.urn)
            .filter((urn): urn is string => Boolean(urn)) || []
    );
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

export interface RelatedEntitiesLists {
    relatedAssets: string[];
    relatedDocuments: string[];
}

/**
 * Computes a document's `relatedAssets` / `relatedDocuments` lists after linking or
 * unlinking a single entity.
 *
 * A document links to normal entities through `relatedAssets` and to other documents
 * through `relatedDocuments`. We only edit the list the entity belongs to — decided by
 * whether its URN is a document URN — and leave the other list untouched. The mutation
 * replaces the full list, so callers pass both back even when only one changed.
 *
 * @param entityUrn - The entity being linked/unlinked from the document
 * @param existingAssetUrns - The document's current relatedAssets URNs
 * @param existingRelatedDocumentUrns - The document's current relatedDocuments URNs
 * @param shouldBeLinked - true to add the entity, false to remove it
 * @returns The next relatedAssets/relatedDocuments lists
 */
export function computeRelatedEntitiesForLinkChange({
    entityUrn,
    existingAssetUrns,
    existingRelatedDocumentUrns,
    shouldBeLinked,
}: {
    entityUrn: string;
    existingAssetUrns: string[];
    existingRelatedDocumentUrns: string[];
    shouldBeLinked: boolean;
}): RelatedEntitiesLists {
    const isDocumentContext = entityUrn.includes(':document:');

    if (shouldBeLinked) {
        return {
            relatedAssets: isDocumentContext ? existingAssetUrns : mergeUrns(existingAssetUrns, [entityUrn]),
            relatedDocuments: isDocumentContext
                ? mergeUrns(existingRelatedDocumentUrns, [entityUrn])
                : existingRelatedDocumentUrns,
        };
    }

    return {
        relatedAssets: isDocumentContext ? existingAssetUrns : existingAssetUrns.filter((urn) => urn !== entityUrn),
        relatedDocuments: isDocumentContext
            ? existingRelatedDocumentUrns.filter((urn) => urn !== entityUrn)
            : existingRelatedDocumentUrns,
    };
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
        title: options?.title || i18next.t('entity.types:document.newDocumentTitle'),
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
 * Splits an array of URNs into document URNs and asset URNs.
 *
 * @param urns - Array of URNs to categorize
 * @returns Object with separate `documentUrns` and `assetUrns` arrays
 */
export function categorizeUrns(urns: string[]): { documentUrns: string[]; assetUrns: string[] } {
    const documentUrns: string[] = [];
    const assetUrns: string[] = [];
    urns.forEach((urn) => {
        if (urn.includes(':document:')) {
            documentUrns.push(urn);
        } else {
            assetUrns.push(urn);
        }
    });
    return { documentUrns, assetUrns };
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
