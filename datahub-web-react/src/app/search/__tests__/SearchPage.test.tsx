import React from 'react';
import { render, waitFor } from '@testing-library/react';
import { InMemoryCache } from '@apollo/client';
import { MockedProvider } from '@apollo/client/testing';
import { Route } from 'react-router';
import { SearchPage } from '../SearchPage';
import TestPageContainer from '../../../utils/test-utils/TestPageContainer';
import { mocksWithSearchFlagsOff } from '../../../Mocks';
import { PageRoutes } from '../../../conf/Global';
import possibleTypesResult from '../../../possibleTypes.generated';

const cache = new InMemoryCache({
    // need to define possibleTypes to allow us to use Apollo cache with union types
    possibleTypes: possibleTypesResult.possibleTypes,
});

describe('SearchPage', () => {
    it('renders the selected filters as checked', async () => {
        const { getByTestId, queryByTestId } = render(
            <MockedProvider mocks={mocksWithSearchFlagsOff} addTypename cache={cache}>
                <TestPageContainer
                    initialEntries={['/search?filter__entityType=DATASET&filter_platform=kafka&page=1&query=test']}
                >
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByTestId('facet-_entityType-DATASET')).toBeInTheDocument());

        const datasetEntityBox = getByTestId('facet-_entityType-DATASET');
        expect(datasetEntityBox).toHaveProperty('checked', true);

        const chartEntityBox = getByTestId('facet-_entityType-CHART');
        expect(chartEntityBox).toHaveProperty('checked', false);
    });

    it('renders the selected filters as checked using legacy URL scheme for entity (entity instead of _entityType)', async () => {
        const { getByTestId, queryByTestId } = render(
            <MockedProvider mocks={mocksWithSearchFlagsOff} addTypename cache={cache}>
                <TestPageContainer
                    initialEntries={['/search?filter_entity=DATASET&filter_platform=kafka&page=1&query=test']}
                >
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByTestId('facet-_entityType-DATASET')).toBeInTheDocument());

        const datasetEntityBox = getByTestId('facet-_entityType-DATASET');
        expect(datasetEntityBox).toHaveProperty('checked', true);

        const chartEntityBox = getByTestId('facet-_entityType-CHART');
        expect(chartEntityBox).toHaveProperty('checked', false);
    });

    it('renders multiple checked filters at once', async () => {
        const { getByTestId, queryByTestId } = render(
            <MockedProvider mocks={mocksWithSearchFlagsOff} addTypename cache={cache}>
                <TestPageContainer
                    initialEntries={['/search?filter__entityType=DATASET&filter_platform=kafka,hdfs&page=1&query=test']}
                >
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByTestId('facet-_entityType-DATASET')).toBeInTheDocument());

        const datasetEntityBox = getByTestId('facet-_entityType-DATASET');
        expect(datasetEntityBox).toHaveProperty('checked', true);

        await waitFor(() => expect(queryByTestId('facet-platform-hdfs')).toBeInTheDocument());
        const hdfsPlatformBox = getByTestId('facet-platform-hdfs');
        expect(hdfsPlatformBox).toHaveProperty('checked', true);
    });
});
