import {
    resolveCollapsedBodyMode,
    shouldPlaceHomeAboveDivider,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/utils/sidebarLayout';

describe('resolveCollapsedBodyMode', () => {
    it('uses the icon rail when icons are supplied', () => {
        expect(resolveCollapsedBodyMode(true)).toBe('icons');
    });

    it('falls back to search-only', () => {
        expect(resolveCollapsedBodyMode(false)).toBe('searchOnly');
    });
});

describe('shouldPlaceHomeAboveDivider', () => {
    it('places home above the divider when there are no filters', () => {
        expect(shouldPlaceHomeAboveDivider({ showFilters: false, hasHomeNav: true })).toBe(true);
    });

    it('keeps home in the tree band when filters are present', () => {
        expect(shouldPlaceHomeAboveDivider({ showFilters: true, hasHomeNav: true })).toBe(false);
    });

    it('is false when there is no home nav', () => {
        expect(shouldPlaceHomeAboveDivider({ showFilters: false, hasHomeNav: false })).toBe(false);
    });
});
