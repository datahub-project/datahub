import { shouldFetchSearchBarTypeahead } from '@app/searchV2/searchBarV2/SearchBarV2.utils';

describe('shouldFetchSearchBarTypeahead', () => {
    it('is false when the bar is inactive, including after the query changes', () => {
        expect(
            shouldFetchSearchBarTypeahead({
                isSearchBarActive: false,
                hasUserTyped: false,
                searchQuery: 'foo',
                pageOpenQuery: 'foo',
            }),
        ).toBe(false);

        expect(
            shouldFetchSearchBarTypeahead({
                isSearchBarActive: false,
                hasUserTyped: true,
                searchQuery: 'foobar',
                pageOpenQuery: 'foo',
            }),
        ).toBe(false);
    });

    it('is false when active but the query still equals the page-open query and the user has not typed', () => {
        expect(
            shouldFetchSearchBarTypeahead({
                isSearchBarActive: true,
                hasUserTyped: false,
                searchQuery: 'foo',
                pageOpenQuery: 'foo',
            }),
        ).toBe(false);
    });

    it('is true when active and the user has typed, or the query differs from the page-open query', () => {
        expect(
            shouldFetchSearchBarTypeahead({
                isSearchBarActive: true,
                hasUserTyped: true,
                searchQuery: 'foo',
                pageOpenQuery: 'foo',
            }),
        ).toBe(true);

        expect(
            shouldFetchSearchBarTypeahead({
                isSearchBarActive: true,
                hasUserTyped: false,
                searchQuery: '',
                pageOpenQuery: 'foo',
            }),
        ).toBe(true);
    });
});
