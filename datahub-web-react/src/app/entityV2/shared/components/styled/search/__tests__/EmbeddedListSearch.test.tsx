import { render } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { EmbeddedListSearch } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearch';
import { UnionType } from '@app/search/utils/constants';

const SELECTED_VIEW_URN = 'urn:li:dataHubView:store-databases';

const searchResultsQueryMock = vi.fn();
const viewQueryMock = vi.fn();
const resultsPropsMock = vi.fn();

vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleQuery: (options: unknown) => {
        searchResultsQueryMock(options);
        return { data: undefined, loading: false, error: undefined, refetch: vi.fn() };
    },
    useGetSearchCountQuery: () => ({ data: undefined, loading: false, error: undefined, refetch: vi.fn() }),
}));
vi.mock('@graphql/view.generated', () => ({
    useGetViewQuery: (options: { skip?: boolean }) => {
        viewQueryMock(options);
        return options.skip
            ? { data: undefined }
            : { data: { view: { __typename: 'DataHubView', urn: SELECTED_VIEW_URN, name: 'Store databases' } } };
    },
}));
vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => ({ localState: { selectedViewUrn: SELECTED_VIEW_URN } }),
}));
vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityContext: () => ({}),
}));
vi.mock('@app/search/utils/useDownloadScrollAcrossEntitiesSearchResults', () => ({
    useDownloadScrollAcrossEntitiesSearchResults: () => ({ refetch: vi.fn() }),
}));
vi.mock('@app/entityV2/shared/components/styled/search/EmbeddedListSearchHeader', () => ({
    default: () => null,
}));
vi.mock('@app/entityV2/shared/components/styled/search/EmbeddedListSearchResults', () => ({
    EmbeddedListSearchResults: (props: unknown) => {
        resultsPropsMock(props);
        return null;
    },
}));

function renderSearch(applyView?: boolean) {
    render(
        <EmbeddedListSearch
            query=""
            page={1}
            unionType={UnionType.AND}
            filters={[]}
            onChangeQuery={vi.fn()}
            onChangeFilters={vi.fn()}
            onChangePage={vi.fn()}
            onChangeUnionType={vi.fn()}
            applyView={applyView}
        />,
    );
}

function lastSearchViewUrn() {
    return searchResultsQueryMock.mock.lastCall?.[0]?.variables?.input?.viewUrn;
}

describe('EmbeddedListSearch selected view', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('ignores the search bar view by default and does not load it', () => {
        renderSearch();

        expect(lastSearchViewUrn()).toBeUndefined();
        expect(viewQueryMock).toHaveBeenLastCalledWith(expect.objectContaining({ skip: true }));
        expect(resultsPropsMock).toHaveBeenLastCalledWith(
            expect.objectContaining({ applyView: false, view: undefined }),
        );
    });

    it('applies the search bar view and loads it when applyView is set', () => {
        renderSearch(true);

        expect(lastSearchViewUrn()).toBe(SELECTED_VIEW_URN);
        expect(viewQueryMock).toHaveBeenLastCalledWith(expect.objectContaining({ skip: false }));
        expect(resultsPropsMock).toHaveBeenLastCalledWith(
            expect.objectContaining({ applyView: true, view: expect.objectContaining({ urn: SELECTED_VIEW_URN }) }),
        );
    });
});
