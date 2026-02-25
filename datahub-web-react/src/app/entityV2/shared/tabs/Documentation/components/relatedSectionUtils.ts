import { ItemType } from '@src/alchemy-components/components/Menu/types';

import { Document, InstitutionalMemoryMetadata } from '@types';

export type RelatedItem =
    | { type: 'link'; data: InstitutionalMemoryMetadata; sortTime: number }
    | { type: 'document'; data: Document; sortTime: number };

/**
 * Combines links and documents into a sorted array by time.
 * Links are sorted by created time, documents by lastModified time.
 *
 * @param links - Array of institutional memory links
 * @param documents - Array of related documents (optional)
 * @returns Sorted array of RelatedItem objects (most recent first)
 */
export function combineAndSortRelatedItems(
    links: InstitutionalMemoryMetadata[],
    documents?: Document[] | null,
): RelatedItem[] {
    const items: RelatedItem[] = [];

    // Add links with their created time
    links.forEach((link) => {
        items.push({
            type: 'link',
            data: link,
            sortTime: link.created?.time || 0,
        });
    });

    // Add documents with their lastModified time
    if (documents && documents.length > 0) {
        documents.forEach((doc) => {
            items.push({
                type: 'document',
                data: doc,
                sortTime: doc.info?.lastModified?.time || 0,
            });
        });
    }

    // Sort by time descending (most recent first)
    return items.sort((a, b) => b.sortTime - a.sortTime);
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
        title: 'Add link',
        icon: 'LinkSimple',
        onClick: options.onAddLink,
        disabled: !options.hasLinkPermissions,
    });

    // Add Context menu item (only shown if feature is enabled, disabled if user can't create documents)
    if (options.isContextDocumentsEnabled) {
        items.push({
            type: 'item',
            key: 'add-context',
            title: 'Add context',
            icon: 'FileText',
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
