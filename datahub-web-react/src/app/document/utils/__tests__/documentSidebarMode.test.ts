import {
    SECONDARY_BROWSE_FILTERS,
    SecondaryBrowseFilter,
    isDocumentSidebarSearchActive,
    isSecondaryBrowseFilter,
    nextPromotedBrowseFilters,
} from '@app/document/utils/documentSidebarMode';

describe('documentSidebarMode', () => {
    describe('isSecondaryBrowseFilter', () => {
        it('accepts known secondary keys only', () => {
            expect(isSecondaryBrowseFilter('tag')).toBe(true);
            expect(isSecondaryBrowseFilter('status')).toBe(true);
            expect(isSecondaryBrowseFilter('term')).toBe(false);
            expect(isSecondaryBrowseFilter('type')).toBe(false);
            expect(SECONDARY_BROWSE_FILTERS).toContain('author');
        });
    });

    describe('isDocumentSidebarSearchActive', () => {
        const empty = {
            searchInput: '',
            typeNames: [] as string[],
            domainUrns: [] as string[],
            tagUrns: [] as string[],
            termUrns: [] as string[],
            authorUrns: [] as string[],
            platformUrns: [] as string[],
            status: 'all' as const,
        };

        it('is inactive when everything is clear', () => {
            expect(isDocumentSidebarSearchActive(empty)).toBe(false);
        });

        it('activates on immediate search input (not only debounced)', () => {
            expect(isDocumentSidebarSearchActive({ ...empty, searchInput: 'run' })).toBe(true);
            expect(isDocumentSidebarSearchActive({ ...empty, searchInput: '   ' })).toBe(false);
        });

        it('activates on any filter including status/author/source', () => {
            expect(isDocumentSidebarSearchActive({ ...empty, typeNames: ['runbook'] })).toBe(true);
            expect(isDocumentSidebarSearchActive({ ...empty, status: 'published' })).toBe(true);
            expect(isDocumentSidebarSearchActive({ ...empty, authorUrns: ['urn:li:corpuser:a'] })).toBe(true);
            expect(isDocumentSidebarSearchActive({ ...empty, platformUrns: ['urn:li:dataPlatform:notion'] })).toBe(
                true,
            );
        });
    });

    describe('nextPromotedBrowseFilters', () => {
        it('is idempotent when nothing new to promote', () => {
            const prev = new Set<SecondaryBrowseFilter>(['status']);
            expect(
                nextPromotedBrowseFilters(prev, {
                    status: 'all',
                    authorUrns: [],
                    platformUrns: [],
                    tagUrns: [],
                }),
            ).toBe(prev);
        });

        it('promotes filters that already have values', () => {
            const prev = new Set<SecondaryBrowseFilter>();
            const next = nextPromotedBrowseFilters(prev, {
                status: 'unpublished',
                authorUrns: ['urn:li:corpuser:a'],
                platformUrns: [],
                tagUrns: ['urn:li:tag:x'],
            });
            expect([...next].sort()).toEqual(['author', 'status', 'tag']);
        });
    });
});
