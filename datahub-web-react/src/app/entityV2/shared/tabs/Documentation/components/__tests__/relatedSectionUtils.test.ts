import { describe, expect, it, vi } from 'vitest';

import {
    combineAndSortRelatedItems,
    createRelatedSectionMenuItems,
    hasRelatedContent,
} from '@app/entityV2/shared/tabs/Documentation/components/relatedSectionUtils';

import { Document, EntityType, InstitutionalMemoryMetadata } from '@types';

// Helper to create test link. `label` doubles as both the display label AND the
// sort key (via `description || label`), so callers pass the desired sort value.
const createTestLink = (url: string, label: string): InstitutionalMemoryMetadata =>
    ({
        url,
        description: label,
        label,
        created: { time: 0, actor: 'urn:li:corpuser:test' },
        author: { urn: 'urn:li:corpuser:test', username: 'test', type: EntityType.CorpUser },
        actor: { urn: 'urn:li:corpuser:test', username: 'test', type: EntityType.CorpUser },
        associatedUrn: 'urn:li:dataset:test',
    }) as InstitutionalMemoryMetadata;

const createTestDocument = (urn: string, title: string): Document =>
    ({
        urn,
        type: EntityType.Document,
        info: {
            title,
            lastModified: { time: 0 },
            created: { time: 0 },
            contents: { text: '' },
        },
    }) as Document;

describe('relatedSectionUtils', () => {
    describe('combineAndSortRelatedItems', () => {
        it('groups documents before links and sorts each group alphabetically (case-insensitive)', () => {
            const links = [
                createTestLink('https://example.com/beta', 'Beta link'),
                createTestLink('https://example.com/alpha', 'alpha link'),
            ];

            const documents = [
                createTestDocument('urn:li:document:2', 'Charlie doc'),
                createTestDocument('urn:li:document:1', 'apple doc'),
            ];

            const result = combineAndSortRelatedItems(links, documents);

            expect(result).toHaveLength(4);
            // Documents first, A→Z (case-insensitive)
            expect(result[0].type).toBe('document');
            expect(result[0].sortLabel).toBe('apple doc');
            expect(result[1].type).toBe('document');
            expect(result[1].sortLabel).toBe('Charlie doc');
            // Then links, A→Z (case-insensitive)
            expect(result[2].type).toBe('link');
            expect(result[2].sortLabel).toBe('alpha link');
            expect(result[3].type).toBe('link');
            expect(result[3].sortLabel).toBe('Beta link');
        });

        it('handles empty documents array', () => {
            const result = combineAndSortRelatedItems([createTestLink('https://example.com', 'foo')], []);
            expect(result).toHaveLength(1);
            expect(result[0].type).toBe('link');
        });

        it('handles null documents', () => {
            const result = combineAndSortRelatedItems([createTestLink('https://example.com', 'foo')], null);
            expect(result).toHaveLength(1);
            expect(result[0].type).toBe('link');
        });

        it('handles undefined documents', () => {
            const result = combineAndSortRelatedItems([createTestLink('https://example.com', 'foo')], undefined);
            expect(result).toHaveLength(1);
            expect(result[0].type).toBe('link');
        });

        it('handles empty links array', () => {
            const result = combineAndSortRelatedItems([], [createTestDocument('urn:li:document:1', 'foo')]);
            expect(result).toHaveLength(1);
            expect(result[0].type).toBe('document');
        });

        it('handles both empty arrays', () => {
            expect(combineAndSortRelatedItems([], [])).toEqual([]);
        });

        it('falls back to url when a link has no description or label', () => {
            const link = createTestLink('https://example.com/only-url', '');
            link.description = null as any;
            link.label = null as any;

            const result = combineAndSortRelatedItems([link], []);
            expect(result[0].sortLabel).toBe('https://example.com/only-url');
        });

        it('uses empty string for documents with no title', () => {
            const doc = createTestDocument('urn:li:document:1', 'ignored');
            (doc.info as any).title = null;

            const result = combineAndSortRelatedItems([], [doc]);
            expect(result[0].sortLabel).toBe('');
        });
    });

    describe('createRelatedSectionMenuItems', () => {
        it('should create both menu items when feature is enabled and user has permissions', () => {
            const onAddLink = vi.fn();
            const onAddContext = vi.fn();

            const result = createRelatedSectionMenuItems({
                onAddLink,
                onAddContext,
                isContextDocumentsEnabled: true,
                hasLinkPermissions: true,
                canCreateDocuments: true,
            });

            expect(result).toHaveLength(2);
            expect(result[0].key).toBe('add-link');
            expect((result[0] as any).title).toBe('Add link');
            expect((result[0] as any).disabled).toBe(false);
            expect(result[1].key).toBe('add-context');
            expect((result[1] as any).title).toBe('Add context');
            expect((result[1] as any).disabled).toBe(false);
        });

        it('should disable Add Link when user has no link permissions', () => {
            const onAddLink = vi.fn();
            const onAddContext = vi.fn();

            const result = createRelatedSectionMenuItems({
                onAddLink,
                onAddContext,
                isContextDocumentsEnabled: true,
                hasLinkPermissions: false,
                canCreateDocuments: true,
            });

            expect((result[0] as any).disabled).toBe(true);
            expect((result[1] as any).disabled).toBe(false);
        });

        it('should disable Add Context when user cannot create documents', () => {
            const onAddLink = vi.fn();
            const onAddContext = vi.fn();

            const result = createRelatedSectionMenuItems({
                onAddLink,
                onAddContext,
                isContextDocumentsEnabled: true,
                hasLinkPermissions: true,
                canCreateDocuments: false,
            });

            expect((result[0] as any).disabled).toBe(false);
            expect((result[1] as any).disabled).toBe(true);
        });

        it('should not include Add Context item when feature is disabled', () => {
            const onAddLink = vi.fn();
            const onAddContext = vi.fn();

            const result = createRelatedSectionMenuItems({
                onAddLink,
                onAddContext,
                isContextDocumentsEnabled: false,
                hasLinkPermissions: true,
                canCreateDocuments: true,
            });

            expect(result).toHaveLength(1);
            expect(result[0].key).toBe('add-link');
        });

        it('should call onAddLink when Add Link is clicked', () => {
            const onAddLink = vi.fn();
            const onAddContext = vi.fn();

            const result = createRelatedSectionMenuItems({
                onAddLink,
                onAddContext,
                isContextDocumentsEnabled: false,
                hasLinkPermissions: true,
                canCreateDocuments: true,
            });

            (result[0] as any).onClick?.();

            expect(onAddLink).toHaveBeenCalledTimes(1);
        });

        it('should call onAddContext when Add Context is clicked', () => {
            const onAddLink = vi.fn();
            const onAddContext = vi.fn();

            const result = createRelatedSectionMenuItems({
                onAddLink,
                onAddContext,
                isContextDocumentsEnabled: true,
                hasLinkPermissions: true,
                canCreateDocuments: true,
            });

            (result[1] as any).onClick?.();

            expect(onAddContext).toHaveBeenCalledTimes(1);
        });

        it('should handle all permissions disabled', () => {
            const onAddLink = vi.fn();
            const onAddContext = vi.fn();

            const result = createRelatedSectionMenuItems({
                onAddLink,
                onAddContext,
                isContextDocumentsEnabled: true,
                hasLinkPermissions: false,
                canCreateDocuments: false,
            });

            expect(result).toHaveLength(2);
            expect((result[0] as any).disabled).toBe(true);
            expect((result[1] as any).disabled).toBe(true);
        });
    });

    describe('hasRelatedContent', () => {
        it('should return true when there are links', () => {
            expect(hasRelatedContent(true, false)).toBe(true);
        });

        it('should return true when there are documents', () => {
            expect(hasRelatedContent(false, true)).toBe(true);
        });

        it('should return true when there are both links and documents', () => {
            expect(hasRelatedContent(true, true)).toBe(true);
        });

        it('should return false when there are neither links nor documents', () => {
            expect(hasRelatedContent(false, false)).toBe(false);
        });
    });
});
