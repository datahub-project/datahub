import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { EmbeddedListSearchResults } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchResults';
import { UnionType } from '@app/search/utils/constants';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { DataHubView, SearchResults } from '@types';

vi.mock('@app/entityV2/shared/components/styled/search/EntitySearchResults', () => ({
    EntitySearchResults: () => null,
}));

const VIEW = { urn: 'urn:li:dataHubView:store-databases', name: 'Store databases' } as DataHubView;
const RESPONSE = { start: 0, count: 10, total: 12, searchResults: [] } as unknown as SearchResults;

function renderResults(props: { applyView?: boolean; view?: DataHubView }) {
    render(
        <MockedProvider mocks={[]}>
            <TestPageContainer>
                <EmbeddedListSearchResults
                    page={1}
                    searchResponse={RESPONSE}
                    filters={[]}
                    selectedFilters={[]}
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
        expect(screen.queryByText('View')).not.toBeInTheDocument();
    });

    it('keeps the All / view switcher for lists that apply the selected view', () => {
        renderResults({ applyView: true, view: VIEW });

        // The view pill and the "Only showing entities in the … view" notice both name it.
        expect(screen.getAllByText('Store databases')).toHaveLength(2);
    });
});
