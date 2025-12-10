import { describe, expect, it, vi } from 'vitest';

import {
    combineAndSortRelatedItems,
    createRelatedSectionMenuItems,
    hasRelatedContent,
} from '@app/entityV2/shared/tabs/Documentation/components/relatedSectionUtils';

import { Document, EntityType, InstitutionalMemoryMetadata } from '@types';

// Helper to create test link
const createTestLink = (url: string, createdTime: number): InstitutionalMemoryMetadata =>
    ({
        url,
        description: `Link to ${url}`,
        label: `Link to ${url}`,
        created: {
            time: createdTime,
            actor: 'urn:li:corpuser:test',
        },
        author: { urn: 'urn:li:corpuser:test', username: 'test', type: EntityType.CorpUser },
        actor: { urn: 'urn:li:corpuser:test', username: 'test', type: EntityType.CorpUser },
        associatedUrn: 'urn:li:dataset:test',
    }) as InstitutionalMemoryMetadata;

// Helper to create test document
const createTestDocument = (urn: string, lastModifiedTime: number): Document =>
    ({
        urn,
        type: EntityType.Document,
        info: {
            title: `Document ${urn}`,
            lastModified: { time: lastModifiedTime },
            created: { time: lastModifiedTime - 1000 },
            contents: { text: '' },
        },
    }) as Document;

describe('relatedSectionUtils', () => {
    describe('combineAndSortRelatedItems', () => {
        it('should combine links and documents and sort by time (most recent first)', () => {
            const links = [
                createTestLink('https://example.com/old', 1000),
                createTestLink('https://example.com/new', 3000),
            ];

            const documents = [
                createTestDocument('urn:li:document:1', 2000),
                createTestDocument('urn:li:document:2', 4000),
            ];

            const result = combineAndSortRelatedItems(links, documents);

            expect(result).toHaveLength(4);
            // Most recent first (4000) -> document:2
            expect(result[0].type).toBe('document');
            expect(result[0].sortTime).toBe(4000);
            // Next (3000) -> link to example.com/new
            expect(result[1].type).toBe('link');
            expect(result[1].sortTime).toBe(3000);
            // Next (2000) -> document:1
            expect(result[2].type).toBe('document');
            expect(result[2].sortTime).toBe(2000);
            // Oldest (1000) -> link to example.com/old
            expect(result[3].type).toBe('link');
            expect(result[3].sortTime).toBe(1000);
        });

        it('should handle empty documents array', () => {
            const links = [createTestLink('https://example.com', 1000)];

            const result = combineAndSortRelatedItems(links, []);

            expect(result).toHaveLength(1);
            expect(result[0].type).toBe('link');
        });

        it('should handle null documents', () => {
            const links = [createTestLink('https://example.com', 1000)];

            const result = combineAndSortRelatedItems(links, null);

            expect(result).toHaveLength(1);
            expect(result[0].type).toBe('link');
        });

        it('should handle undefined documents', () => {
            const links = [createTestLink('https://example.com', 1000)];

            const result = combineAndSortRelatedItems(links, undefined);

            expect(result).toHaveLength(1);
            expect(result[0].type).toBe('link');
        });

        it('should handle empty links array', () => {
            const documents = [createTestDocument('urn:li:document:1', 2000)];

            const result = combineAndSortRelatedItems([], documents);

            expect(result).toHaveLength(1);
            expect(result[0].type).toBe('document');
        });

        it('should handle both empty arrays', () => {
            const result = combineAndSortRelatedItems([], []);

            expect(result).toEqual([]);
        });

        it('should use 0 as default sortTime when created time is missing for links', () => {
            const linkWithoutTime = {
                ...createTestLink('https://example.com', 1000),
                created: null as any,
            };

            const result = combineAndSortRelatedItems([linkWithoutTime], []);

            expect(result[0].sortTime).toBe(0);
        });

        it('should use 0 as default sortTime when lastModified time is missing for documents', () => {
            const docWithoutTime = createTestDocument('urn:li:document:1', 2000);
            (docWithoutTime.info as any).lastModified = null;

            const result = combineAndSortRelatedItems([], [docWithoutTime]);

            expect(result[0].sortTime).toBe(0);
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
