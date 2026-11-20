import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { SearchSelect } from '@app/entityV2/shared/components/styled/search/SearchSelect';
import { ENTITY_FILTER_NAME } from '@app/search/utils/constants';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { EntityRegistryContext } from '@src/entityRegistryContext';
import { EntityType, FacetFilterInput } from '@src/types.generated';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';

// What the caller allows — every real caller passes the full set it supports, which is
// what made the widening bug invisible.
const FIXED_TYPES = [EntityType.Dataset, EntityType.Chart, EntityType.Dashboard];

let lastInput: any;

vi.mock('@graphql/search.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/search.generated')>()),
    useGetSearchResultsForMultipleQuery: ({ variables }) => {
        lastInput = variables.input;
        return { data: undefined, loading: false, error: undefined, refetch: vi.fn() };
    },
}));

// Stand in for the results pane so the test can drive the entity-type facet directly.
vi.mock('@app/entityV2/shared/components/styled/search/EmbeddedListSearchResults', () => ({
    EmbeddedListSearchResults: ({ onChangeFilters }: { onChangeFilters: (filters: FacetFilterInput[]) => void }) => (
        <button type="button" onClick={() => onChangeFilters([{ field: ENTITY_FILTER_NAME, values: ['DATASET'] }])}>
            pick dataset
        </button>
    ),
}));

vi.mock('@app/search/SearchBar', () => ({ SearchBar: () => null }));
vi.mock('@src/app/searchV2/sorting/SearchSortSelect', () => ({ default: () => null }));
vi.mock('@app/entityV2/shared/components/styled/search/SearchSelectBar', () => ({
    SearchSelectBar: () => null,
}));

const entityTypeFilterValues = (input) =>
    input.orFilters?.flatMap((or) => or.and.filter((f) => f.field === ENTITY_FILTER_NAME)).flatMap((f) => f.values) ??
    [];

describe('SearchSelect', () => {
    const renderSelect = () =>
        render(
            <CustomThemeProvider>
                <EntityRegistryContext.Provider value={getTestEntityRegistry()}>
                    <SearchSelect fixedEntityTypes={FIXED_TYPES} selectedEntities={[]} setSelectedEntities={vi.fn()} />
                </EntityRegistryContext.Provider>
            </CustomThemeProvider>,
        );

    it('searches the caller-supplied types when nothing is selected', () => {
        renderSelect();

        expect(lastInput.types).toEqual(FIXED_TYPES);
        expect(entityTypeFilterValues(lastInput)).toEqual([]);
    });

    it('narrows to the picked type instead of widening the search', () => {
        renderSelect();
        fireEvent.click(screen.getByText('pick dataset'));

        // The scope the caller allows is unchanged; the picked type narrows via the facet.
        expect(lastInput.types).toEqual(FIXED_TYPES);
        expect(entityTypeFilterValues(lastInput)).toEqual([EntityType.Dataset]);
    });
});
