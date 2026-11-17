import {
    SECONDARY_BROWSE_FILTERS,
    SecondaryBrowseFilter,
    isMetricsSidebarSearchActive,
    isSecondaryBrowseFilter,
    nextPromotedBrowseFilters,
} from '@app/metrics/utils/metricsSidebarMode';

describe('metricsSidebarMode', () => {
    describe('isSecondaryBrowseFilter', () => {
        it('accepts known secondary keys only', () => {
            expect(isSecondaryBrowseFilter('tag')).toBe(true);
            expect(isSecondaryBrowseFilter('owners')).toBe(true);
            expect(isSecondaryBrowseFilter('term')).toBe(false);
            expect(isSecondaryBrowseFilter('platform')).toBe(false);
            expect(SECONDARY_BROWSE_FILTERS).toContain('owners');
        });
    });

    describe('isMetricsSidebarSearchActive', () => {
        const empty = {
            searchInput: '',
            platformUrns: [] as string[],
            domainUrns: [] as string[],
            tagUrns: [] as string[],
            termUrns: [] as string[],
            ownerUrns: [] as string[],
        };

        it('is inactive when everything is clear', () => {
            expect(isMetricsSidebarSearchActive(empty)).toBe(false);
        });

        it('activates on immediate search input (not only debounced)', () => {
            expect(isMetricsSidebarSearchActive({ ...empty, searchInput: 'revenue' })).toBe(true);
            expect(isMetricsSidebarSearchActive({ ...empty, searchInput: '   ' })).toBe(false);
        });

        it('activates on any filter', () => {
            expect(isMetricsSidebarSearchActive({ ...empty, platformUrns: ['urn:li:dataPlatform:dbt'] })).toBe(true);
            expect(isMetricsSidebarSearchActive({ ...empty, domainUrns: ['urn:li:domain:eng'] })).toBe(true);
            expect(isMetricsSidebarSearchActive({ ...empty, tagUrns: ['urn:li:tag:pii'] })).toBe(true);
            expect(isMetricsSidebarSearchActive({ ...empty, termUrns: ['urn:li:glossaryTerm:rev'] })).toBe(true);
            expect(isMetricsSidebarSearchActive({ ...empty, ownerUrns: ['urn:li:corpuser:a'] })).toBe(true);
        });
    });

    describe('nextPromotedBrowseFilters', () => {
        it('is idempotent when nothing new to promote', () => {
            const prev = new Set<SecondaryBrowseFilter>(['tag']);
            expect(
                nextPromotedBrowseFilters(prev, {
                    tagUrns: [],
                    ownerUrns: [],
                }),
            ).toBe(prev);
        });

        it('promotes filters that already have values', () => {
            const prev = new Set<SecondaryBrowseFilter>();
            const next = nextPromotedBrowseFilters(prev, {
                tagUrns: ['urn:li:tag:x'],
                ownerUrns: ['urn:li:corpuser:a'],
            });
            expect([...next].sort()).toEqual(['owners', 'tag']);
        });
    });
});
