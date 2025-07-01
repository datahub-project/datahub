import { InMemoryCache } from '@apollo/client';
import { MockedProvider } from '@apollo/client/testing';
import { render, waitFor } from '@testing-library/react';
import React from 'react';
import { Route } from 'react-router';

import { SearchPage } from '@app/searchV2/SearchPage';
import { PageRoutes } from '@conf/Global';
import { mocksWithSearchFlagsOff } from '@src/Mocks';
import possibleTypesResult from '@src/possibleTypes.generated';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const cache = new InMemoryCache({
    // need to define possibleTypes to allow us to use Apollo cache with union types
    possibleTypes: possibleTypesResult.possibleTypes,
});

describe('SearchPage', () => {
    const URL =
        '/search' +
        '?filter__entityType␞typeNames___false___EQUAL___0=DATASET' +
        '&filter_platform___false___EQUAL___1=urn%3Ali%3AdataPlatform%3Asnowflake,urn%3Ali%3AdataPlatform%3Atableau' +
        '&page=1' +
        '&query=test';

    it('renders the selected filters as checked', async () => {
        const { getByTestId, queryByTestId } = render(
            <MockedProvider mocks={mocksWithSearchFlagsOff} addTypename cache={cache}>
                <TestPageContainer initialEntries={[URL]}>
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByTestId('active-filter-_entityType␞typeNames')).toBeInTheDocument());
        const datasetEntityValue = getByTestId('active-filter-value-_entityType␞typeNames-DATASET');
        expect(datasetEntityValue).toBeInTheDocument();
        const chartEntityValue = queryByTestId('active-filter-value-_entityType␞typeNames-CHART');
        expect(chartEntityValue).not.toBeInTheDocument();
    });

    it('renders multiple checked filters at once', async () => {
        const { getByTestId, queryByTestId } = render(
            <MockedProvider mocks={mocksWithSearchFlagsOff} addTypename cache={cache}>
                <TestPageContainer initialEntries={[URL]}>
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByTestId('active-filter-_entityType␞typeNames')).toBeInTheDocument());
        const datasetEntityValue = getByTestId('active-filter-value-_entityType␞typeNames-DATASET');
        expect(datasetEntityValue).toBeInTheDocument();

        await waitFor(() => expect(queryByTestId('active-filter-platform')).toBeInTheDocument());
        const snowflakePlatformValue = getByTestId('active-filter-value-platform-urn:li:dataPlatform:snowflake');
        const tableauPlatformValue = getByTestId('active-filter-value-platform-urn:li:dataPlatform:tableau');
        expect(snowflakePlatformValue).toBeInTheDocument();
        expect(tableauPlatformValue).toBeInTheDocument();
        const bigqueryPlatformValue = queryByTestId('active-filter-value-platform-urn:li:dataPlatform:bigquery');
        expect(bigqueryPlatformValue).not.toBeInTheDocument();
    });
});
