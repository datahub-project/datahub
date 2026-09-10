import {
    SECONDARY_BROWSE_FILTERS,
    SecondaryBrowseFilter,
    buildDataProductsSearchModeState,
    isDataProductsSidebarSearchActive,
    isSecondaryBrowseFilter,
    dataProductsSidebarSearchQuery,
    nextPromotedBrowseFilters,
} from '@app/dataProducts/utils/dataProductsSidebarMode';

describe('dataProductsSidebarMode', () => {
    describe('dataProductsSidebarSearchQuery', () => {
        it('uses wildcard when query is blank', () => {
            expect(dataProductsSidebarSearchQuery('')).toBe('*');
            expect(dataProductsSidebarSearchQuery('   ')).toBe('*');
        });

        it('trims non-empty query text', () => {
            expect(dataProductsSidebarSearchQuery('  revenue  ')).toBe('revenue');
        });
    });

    describe('isSecondaryBrowseFilter', () => {
        it('accepts known secondary keys only', () => {
            expect(isSecondaryBrowseFilter('term')).toBe(true);
            expect(isSecondaryBrowseFilter('application')).toBe(true);
            expect(isSecondaryBrowseFilter('tag')).toBe(false);
            expect(isSecondaryBrowseFilter('domain')).toBe(false);
            expect(SECONDARY_BROWSE_FILTERS).toContain('application');
        });
    });

    describe('isDataProductsSidebarSearchActive', () => {
        const empty = {
            searchInput: '',
            domainUrns: [] as string[],
            tagUrns: [] as string[],
            termUrns: [] as string[],
            ownerUrns: [] as string[],
            applicationUrns: [] as string[],
        };

        it('is inactive when everything is clear', () => {
            expect(isDataProductsSidebarSearchActive(empty)).toBe(false);
        });

        it('activates on immediate search input (not only debounced)', () => {
            expect(isDataProductsSidebarSearchActive({ ...empty, searchInput: 'orders' })).toBe(true);
            expect(isDataProductsSidebarSearchActive({ ...empty, searchInput: '   ' })).toBe(false);
        });

        it('activates on any filter', () => {
            expect(isDataProductsSidebarSearchActive({ ...empty, domainUrns: ['urn:li:domain:eng'] })).toBe(true);
            expect(isDataProductsSidebarSearchActive({ ...empty, ownerUrns: ['urn:li:corpuser:a'] })).toBe(true);
            expect(isDataProductsSidebarSearchActive({ ...empty, tagUrns: ['urn:li:tag:pii'] })).toBe(true);
            expect(isDataProductsSidebarSearchActive({ ...empty, termUrns: ['urn:li:glossaryTerm:rev'] })).toBe(true);
            expect(isDataProductsSidebarSearchActive({ ...empty, applicationUrns: ['urn:li:application:app'] })).toBe(
                true,
            );
        });
    });

    describe('buildDataProductsSearchModeState', () => {
        const filters = {
            domainUrns: [] as string[],
            tagUrns: [] as string[],
            termUrns: [] as string[],
            ownerUrns: [] as string[],
            applicationUrns: [] as string[],
        };

        it('uses immediate input for chrome and debounced input for fetch', () => {
            const state = buildDataProductsSearchModeState({
                searchInput: 'rev',
                debouncedSearchInput: '',
                filters,
            });
            expect(state.isSearchActive).toBe(true);
            expect(state.shouldFetchSearch).toBe(false);
            expect(state.searchQuery).toBe('*');
        });
    });

    describe('nextPromotedBrowseFilters', () => {
        it('is idempotent when nothing new to promote', () => {
            const prev = new Set<SecondaryBrowseFilter>(['term']);
            expect(
                nextPromotedBrowseFilters(prev, {
                    termUrns: [],
                    applicationUrns: [],
                }),
            ).toBe(prev);
        });

        it('promotes filters that already have values', () => {
            const prev = new Set<SecondaryBrowseFilter>();
            const next = nextPromotedBrowseFilters(prev, {
                termUrns: ['urn:li:glossaryTerm:rev'],
                applicationUrns: ['urn:li:application:app'],
            });
            expect([...next].sort()).toEqual(['application', 'term']);
        });
    });
});
