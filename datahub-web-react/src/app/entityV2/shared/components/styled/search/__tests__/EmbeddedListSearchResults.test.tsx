import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { EmbeddedListSearchResults } from '@app/entityV2/shared/components/styled/search/EmbeddedListSearchResults';
import { UnionType } from '@app/search/utils/constants';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { DataHubView, SearchResults } from '@types';

vi.mock('@app/entityV2/shared/components/styled/search/EntitySearchResults', () => ({
    EntitySearchResults: () => null,
}));

const { navigateToSearchUrl } = vi.hoisted(() => ({ navigateToSearchUrl: vi.fn() }));
vi.mock('@app/searchV2/utils/navigateToSearchUrl', () => ({ navigateToSearchUrl }));

const VIEW = { urn: 'urn:li:dataHubView:store-databases', name: 'Store databases' } as DataHubView;
const RESPONSE = { start: 0, count: 10, total: 12, searchResults: [] } as unknown as SearchResults;

const HDFS_URN = 'urn:li:dataPlatform:hdfs';
const INGESTED_RESPONSE = {
    start: 0,
    count: 10,
    total: 1,
    searchResults: [
        {
            entity: { urn: 'urn:li:dataset:1', type: 'DATASET', platform: { urn: HDFS_URN, name: 'hdfs' } },
            matchedFields: [],
        },
    ],
    facets: [{ field: 'platform', aggregations: [{ value: HDFS_URN, count: 1 }] }],
} as unknown as SearchResults;

function renderResults(props: {
    applyView?: boolean;
    view?: DataHubView;
    searchResponse?: SearchResults;
    isViewAllMode?: boolean;
    handleViewAllClickWarning?: () => void;
}) {
    render(
        <MockedProvider mocks={[]}>
            <TestPageContainer>
                <EmbeddedListSearchResults
                    page={1}
                    searchResponse={props.searchResponse ?? RESPONSE}
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
                    isViewAllMode={props.isViewAllMode}
                    handleViewAllClickWarning={props.handleViewAllClickWarning}
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

        expect(screen.getByText('View')).toBeInTheDocument();
        expect(screen.getByText('All').parentElement).toHaveTextContent('All12');
        expect(screen.getByText('Store databases', { selector: 'span' }).parentElement).toHaveTextContent(
            'Store databases8',
        );
        expect(screen.getByText(/Only showing entities in the/)).toHaveTextContent(
            'Only showing entities in the Store databases view.',
        );
    });
});

describe('EmbeddedListSearchResults view-all warning', () => {
    it('shows no warning outside view-all mode', () => {
        renderResults({ searchResponse: INGESTED_RESPONSE });

        expect(screen.queryByText('Results may be incomplete.')).not.toBeInTheDocument();
    });

    it('links to a search of all assets on the platform', () => {
        const handleViewAllClickWarning = vi.fn();
        renderResults({ searchResponse: INGESTED_RESPONSE, isViewAllMode: true, handleViewAllClickWarning });

        expect(screen.getByText(/Results may be incomplete\./)).toBeInTheDocument();
        fireEvent.click(screen.getByRole('button', { name: 'Search all ingested hdfs assets' }));

        expect(handleViewAllClickWarning).toHaveBeenCalled();
        expect(navigateToSearchUrl).toHaveBeenCalledWith(
            expect.objectContaining({ filters: [{ field: 'platform', values: [HDFS_URN] }] }),
        );
    });
});
