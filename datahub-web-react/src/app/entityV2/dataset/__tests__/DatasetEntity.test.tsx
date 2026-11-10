import { InMemoryCache } from '@apollo/client';
import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';

import { DatasetEntity } from '@app/entityV2/dataset/DatasetEntity';
import { hexToRgb } from '@app/sharedV2/colors/colorUtils';
import lightTheme from '@conf/theme/colorThemes/light';
import possibleTypesResult from '@src/possibleTypes.generated';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { GetSearchResultsForMultipleDocument, GetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { Dataset, EntityType, SearchResult } from '@types';

const SIXTY_DAYS_AGO = Date.now() - 60 * 24 * 60 * 60 * 1000;
const AGE_RED = 'rgb(245, 34, 45)';
const NEUTRAL = `rgb(${hexToRgb(lightTheme.icon).join(', ')})`;

function searchCardClockColor(name: string, typeNames: string[], viewProperties: { materialized: boolean } | null) {
    const dataset = {
        urn: `urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.${name},PROD)`,
        type: EntityType.Dataset,
        name,
        platform: { urn: 'urn:li:dataPlatform:snowflake', type: EntityType.DataPlatform, name: 'snowflake' },
        properties: { name, lastModified: { time: SIXTY_DAYS_AGO } },
        subTypes: { typeNames },
        viewProperties,
    } as unknown as Dataset;
    const result = { entity: dataset, matchedFields: [] } as unknown as SearchResult;
    render(
        <MockedProvider mocks={[]}>
            <TestPageContainer>{new DatasetEntity().renderSearch(result)}</TestPageContainer>
        </MockedProvider>,
    );
    return getComputedStyle(screen.getByTestId('freshness').querySelector('svg') as SVGElement).color;
}

describe('DatasetEntity search card freshness clock', () => {
    it('shows the time of a view in the neutral icon color', () => {
        expect(searchCardClockColor('my_view', ['View'], { materialized: false })).toBe(NEUTRAL);
    });

    it('colors an old table time by its age', () => {
        expect(searchCardClockColor('my_table', ['Table'], null)).toBe(AGE_RED);
    });
});

describe('DatasetEntity search card data from the main search query', () => {
    // Main search loads its results with this query. Apollo returns only the fields that the query selects.
    it('keeps viewProperties for a dataset and for its sibling', () => {
        const cache = new InMemoryCache({ possibleTypes: possibleTypesResult.possibleTypes });
        const variables = { input: { query: '*', start: 0, count: 10 }, skipSiblingsSearch: false };
        const view = (name: string) => ({
            __typename: 'Dataset',
            urn: `urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.${name},PROD)`,
            type: EntityType.Dataset,
            viewProperties: { __typename: 'ViewProperties', materialized: false },
        });
        const siblingsSearch = {
            __typename: 'ScrollResults',
            count: 1,
            total: 1,
            searchResults: [{ __typename: 'SearchResult', entity: view('my_view_sibling') }],
        };
        cache.writeQuery({
            query: GetSearchResultsForMultipleDocument,
            variables,
            data: {
                searchAcrossEntities: {
                    __typename: 'SearchResults',
                    start: 0,
                    count: 1,
                    total: 1,
                    searchResults: [{ __typename: 'SearchResult', entity: { ...view('my_view'), siblingsSearch } }],
                },
            },
        });
        const { result } = cache.diff<GetSearchResultsForMultipleQuery>({
            query: GetSearchResultsForMultipleDocument,
            variables,
            returnPartialData: true,
            optimistic: false,
        });
        const entity = result?.searchAcrossEntities?.searchResults[0]?.entity as Dataset | undefined;
        const sibling = entity?.siblingsSearch?.searchResults[0]?.entity as Dataset | undefined;
        expect(entity?.viewProperties?.materialized).toBe(false);
        expect(sibling?.viewProperties?.materialized).toBe(false);
    });
});
