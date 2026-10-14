import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { SearchResults } from '@app/searchV2/SearchResults';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { EntityRegistryContext } from '@src/entityRegistryContext';

import { Entity, EntityType } from '@types';

vi.mock('@app/searchV2/useSearchAndBrowseVersion', () => ({
    useIsSearchV2: () => true,
    useIsBrowseV2: () => false,
}));

vi.mock('@app/useShowNavBarRedesign', () => ({
    useShowNavBarRedesign: () => false,
}));

vi.mock('@app/useAppConfig', () => ({
    useIsShowSeparateSiblingsEnabled: () => true,
    useAppConfig: () => ({ loaded: false, config: { visualConfig: {} } }),
}));

vi.mock('@app/searchV2/useSearchResultLineageCounts', () => ({
    useSearchResultLineageCounts: () => ({
        countsByUrn: new Map(),
        loading: false,
        error: undefined,
    }),
}));

const FIRST_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,sample.events,PROD)';
const LAST_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,sample.orders,PROD)';

function searchResult(urn: string) {
    return {
        entity: { urn, type: EntityType.Dataset } as Entity,
        matchedFields: [],
    };
}

const entityRegistry = {
    renderSearchResult: () => <div data-testid="search-result-card" />,
    renderProfile: (_type: EntityType, urn: string) => <div data-testid="search-result-profile">{urn}</div>,
};

function renderResults() {
    return render(
        <MemoryRouter>
            <CustomThemeProvider>
                <EntityRegistryContext.Provider value={entityRegistry as never}>
                    <SearchResults
                        loading={false}
                        query="events"
                        page={1}
                        searchResponse={{
                            start: 0,
                            count: 2,
                            total: 2,
                            searchResults: [searchResult(FIRST_URN), searchResult(LAST_URN)],
                        }}
                        selectedFilters={[]}
                        error={undefined}
                        onChangeFilters={() => undefined}
                        onChangePage={() => undefined}
                        numResultsPerPage={10}
                        setNumResultsPerPage={() => undefined}
                        isSelectMode={false}
                        selectedEntities={[]}
                        suggestions={[]}
                        setSelectedEntities={() => undefined}
                        setIsSelectMode={() => undefined}
                        onChangeSelectAll={() => undefined}
                        refetch={() => undefined}
                    />
                </EntityRegistryContext.Provider>
            </CustomThemeProvider>
        </MemoryRouter>,
    );
}

describe('SearchResults profile sidebar', () => {
    beforeEach(() => {
        Element.prototype.scrollIntoView = vi.fn();
    });

    it('keeps the profile closed until click or ArrowDown', () => {
        renderResults();

        expect(screen.queryByTestId('search-result-profile')).not.toBeInTheDocument();

        fireEvent.mouseEnter(screen.getAllByTestId('search-result')[0]);
        expect(screen.queryByTestId('search-result-profile')).not.toBeInTheDocument();

        fireEvent.click(screen.getAllByTestId('search-result')[1]);
        expect(screen.getByTestId('search-result-profile')).toHaveTextContent(LAST_URN);

        fireEvent.keyDown(document.body, { key: 'Escape' });
        expect(screen.queryByTestId('search-result-profile')).not.toBeInTheDocument();

        fireEvent.keyDown(document.body, { key: 'ArrowDown' });
        expect(screen.getByTestId('search-result-profile')).toHaveTextContent(FIRST_URN);

        fireEvent.keyDown(document.body, { key: 'ArrowDown' });
        expect(screen.getByTestId('search-result-profile')).toHaveTextContent(LAST_URN);
    });

    it('selects the last row on ArrowUp from a closed sidebar and clears on Escape', () => {
        renderResults();

        fireEvent.keyDown(document.body, { key: 'ArrowUp' });
        expect(screen.getByTestId('search-result-profile')).toHaveTextContent(LAST_URN);

        fireEvent.keyDown(document.body, { key: 'ArrowUp' });
        expect(screen.getByTestId('search-result-profile')).toHaveTextContent(FIRST_URN);

        fireEvent.keyDown(document.body, { key: 'Escape' });
        expect(screen.queryByTestId('search-result-profile')).not.toBeInTheDocument();
    });
});
