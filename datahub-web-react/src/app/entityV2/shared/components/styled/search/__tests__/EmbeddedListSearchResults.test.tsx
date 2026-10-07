import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { EmbeddedListSearchResults } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchResults';
import { UnionType } from '@app/search/utils/constants';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { DataHubView, FacetFilterInput, SearchResults } from '@types';

const selectedFiltersMock = vi.fn();

vi.mock('@app/entityV2/shared/components/styled/search/EntitySearchResults', () => ({
    EntitySearchResults: () => null,
}));
vi.mock('@app/searchV2/filters/SelectedSearchFilters', () => ({
    default: (props: { selectedFilters: FacetFilterInput[] }) => {
        selectedFiltersMock(props);
        return <div data-testid="selected-filters" />;
    },
}));

const VIEW = { urn: 'urn:li:dataHubView:store-databases', name: 'Store databases' } as DataHubView;
const RESPONSE = { start: 0, count: 10, total: 12, searchResults: [] } as unknown as SearchResults;

function renderResults(props: { applyView?: boolean; view?: DataHubView; selectedFilters?: FacetFilterInput[] }) {
    render(
        <MockedProvider mocks={[]}>
            <TestPageContainer>
                <EmbeddedListSearchResults
                    page={1}
                    searchResponse={RESPONSE}
                    filters={[]}
                    selectedFilters={props.selectedFilters ?? []}
                    loading={false}
                    unionType={UnionType.AND}
                    onChangeUnionType={vi.fn()}
                    onChangeFilters={vi.fn()}
                    onChangePage={vi.fn()}
                    isSelectMode={false}
                    selectedEntities={[]}
                    setSelectedEntities={vi.fn()}
                    numResultsPerPage={10}
                    setNumResultsPerPage={vi.fn()}
                    applyView={props.applyView}
                    view={props.view}
                    selectedViewUrn={props.view?.urn}
                    defaultViewUrn={props.view?.urn}
                    defaultViewCount={8}
                    allSearchCount={12}
                />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('EmbeddedListSearchResults view row', () => {
    it('shows no view row when the list does not apply the selected view', () => {
        renderResults({ view: VIEW });

        expect(screen.queryByText('Store databases')).not.toBeInTheDocument();
        expect(screen.queryByTestId('embedded-list-active-filters')).not.toBeInTheDocument();
    });

    it('shows only the filters added in the list when it does not apply the selected view', () => {
        const selectedFilters = [{ field: 'platform', values: ['urn:li:dataPlatform:postgres'] }];
        renderResults({ view: VIEW, selectedFilters });

        expect(screen.getByTestId('embedded-list-active-filters')).toBeInTheDocument();
        expect(selectedFiltersMock).toHaveBeenLastCalledWith(expect.objectContaining({ selectedFilters }));
        expect(screen.queryByText('Store databases')).not.toBeInTheDocument();
    });

    it('keeps the All / view switcher for lists that apply the selected view', () => {
        renderResults({ applyView: true, view: VIEW });

        expect(screen.queryByTestId('embedded-list-active-filters')).not.toBeInTheDocument();
        // The view pill and the "Only showing entities in the … view" notice both name it.
        expect(screen.getAllByText('Store databases')).toHaveLength(2);
    });
});
