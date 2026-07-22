import { FileText } from '@phosphor-icons/react/dist/csr/FileText';
import { LinkSimple } from '@phosphor-icons/react/dist/csr/LinkSimple';
import i18next from 'i18next';

import { ItemType } from '@src/alchemy-components/components/Menu/types';

import { Document, InstitutionalMemoryMetadata } from '@types';

export type RelatedItem =
    | { type: 'link'; data: InstitutionalMemoryMetadata; sortLabel: string }
    | { type: 'document'; data: Document; sortLabel: string };

/**
 * Extracts the user-visible label for a link — mirrors the label shown by
 * `ResourceLinkPill` so sort order matches on-screen order.
 */
function getLinkSortLabel(link: InstitutionalMemoryMetadata): string {
    return link.description || link.label || link.url || '';
}

/**
 * Extracts the user-visible title for a document — mirrors what
 * `ResourceDocumentPill` renders as the pill label.
 */
function getDocumentSortLabel(doc: Document): string {
    return doc.info?.title || '';
}

/**
 * Combines links and documents into a sorted array. Order is:
 *   1. Documents first, links second (grouped by type).
 *   2. Alphabetical (case-insensitive, locale-aware) within each group.
 *
 * Chosen because most users scan Resources for a specific doc by name; grouping
 * docs together and sorting alphabetically makes the list predictable and
 * scannable at a glance.
 *
 * @param links - Array of institutional memory links
 * @param documents - Array of related documents (optional)
 * @returns Sorted array of RelatedItem objects (documents A→Z, then links A→Z)
 */
export function combineAndSortRelatedItems(
    links: InstitutionalMemoryMetadata[],
    documents?: Document[] | null,
): RelatedItem[] {
    const items: RelatedItem[] = [];

    if (documents && documents.length > 0) {
        documents.forEach((doc) => {
            items.push({ type: 'document', data: doc, sortLabel: getDocumentSortLabel(doc) });
        });
    }

    links.forEach((link) => {
        items.push({ type: 'link', data: link, sortLabel: getLinkSortLabel(link) });
    });

    return items.sort((a, b) => {
        if (a.type !== b.type) return a.type === 'document' ? -1 : 1;
        return a.sortLabel.localeCompare(b.sortLabel, undefined, { sensitivity: 'base' });
    });
}

/**
 * Creates menu items for the RelatedSection add menu.
 *
 * @param options - Configuration options
 * @param options.onAddLink - Callback when Add Link is clicked
 * @param options.onAddContext - Callback when Add Context is clicked
 * @param options.isContextDocumentsEnabled - Whether context documents feature is enabled
 * @param options.hasLinkPermissions - Whether user has permission to edit links
 * @param options.canCreateDocuments - Whether user has permission to create documents (manageDocuments platform privilege)
 * @returns Array of menu items
 */
export function createRelatedSectionMenuItems(options: {
    onAddLink: () => void;
    onAddContext: () => void;
    isContextDocumentsEnabled: boolean;
    hasLinkPermissions: boolean;
    canCreateDocuments: boolean;
}): ItemType[] {
    const items: ItemType[] = [];

    // Add Link menu item (always shown, but may be disabled)
    items.push({
        type: 'item',
        key: 'add-link',
        title: i18next.t('entity.profile.documentation:addLink'),
        icon: LinkSimple,
        onClick: options.onAddLink,
        disabled: !options.hasLinkPermissions,
    });

    // Add Context menu item (only shown if feature is enabled, disabled if user can't create documents)
    if (options.isContextDocumentsEnabled) {
        items.push({
            type: 'item',
            key: 'add-context',
            title: i18next.t('entity.profile.documentation:addContext'),
            icon: FileText,
            onClick: options.onAddContext,
            disabled: !options.canCreateDocuments,
        });
    }

    return items;
}

/**
 * Checks if there is any content to display (links or documents).
 *
 * @param hasLinks - Whether there are any links
 * @param hasDocuments - Whether there are any documents
 * @returns True if there is content to display
 */
export function hasRelatedContent(hasLinks: boolean, hasDocuments: boolean): boolean {
    return hasLinks || hasDocuments;
}
