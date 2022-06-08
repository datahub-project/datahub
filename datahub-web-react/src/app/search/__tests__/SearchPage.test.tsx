import React from 'react';
import { act } from 'react-dom/test-utils';
import { fireEvent, render, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import { Route } from 'react-router';

import { SearchPage } from '../SearchPage';
import TestPageContainer from '../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../Mocks';
import { PageRoutes } from '../../../conf/Global';

describe('SearchPage', () => {
    it('renders loading', async () => {
        const promise = Promise.resolve();
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer
                    initialEntries={['/search?filter_entity=DATASET&filter_platform=hive,kafka&page=1&query=sample']}
                >
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(getByText('Loading...')).toBeInTheDocument());
        await act(() => promise);
    });

    it('renders the selected filters as checked', async () => {
        const { getByTestId, queryByTestId } = render(
            <MockedProvider
                mocks={mocks}
                addTypename={false}
                defaultOptions={{
                    watchQuery: { fetchPolicy: 'no-cache' },
                    query: { fetchPolicy: 'no-cache' },
                }}
            >
                <TestPageContainer
                    initialEntries={['/search?filter_entity=DATASET&filter_platform=kafka&page=1&query=test']}
                >
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByTestId('facet-entity-DATASET')).toBeInTheDocument());

        const datasetEntityBox = getByTestId('facet-entity-DATASET');
        expect(datasetEntityBox).toHaveProperty('checked', true);

        const chartEntityBox = getByTestId('facet-entity-CHART');
        expect(chartEntityBox).toHaveProperty('checked', false);
    });

    it('renders multiple checked filters at once', async () => {
        const { getByTestId, queryByTestId } = render(
            <MockedProvider
                mocks={mocks}
                addTypename={false}
                defaultOptions={{
                    watchQuery: { fetchPolicy: 'no-cache' },
                    query: { fetchPolicy: 'no-cache' },
                }}
            >
                <TestPageContainer
                    initialEntries={['/search?filter_entity=DATASET&filter_platform=kafka,hdfs&page=1&query=test']}
                >
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByTestId('facet-entity-DATASET')).toBeInTheDocument());

        const datasetEntityBox = getByTestId('facet-entity-DATASET');
        expect(datasetEntityBox).toHaveProperty('checked', true);

        const expandButton = getByTestId('expand-facet-platform');
        act(() => {
            fireEvent.click(expandButton);
        });

        await waitFor(() => expect(queryByTestId('facet-platform-hdfs')).toBeInTheDocument());
        const hdfsPlatformBox = getByTestId('facet-platform-hdfs');
        expect(hdfsPlatformBox).toHaveProperty('checked', true);
    });

    it('clicking a filter selects a new filter', async () => {
        const promise = Promise.resolve();
        const { getByTestId, queryByTestId } = render(
            <MockedProvider
                mocks={mocks}
                addTypename={false}
                defaultOptions={{
                    watchQuery: { fetchPolicy: 'no-cache' },
                    query: { fetchPolicy: 'no-cache' },
                }}
            >
                <TestPageContainer
                    initialEntries={['/search?filter_entity=DATASET&filter_platform=kafka&page=1&query=test']}
                >
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByTestId('facet-entity-DATASET')).toBeInTheDocument());

        const datasetEntityBox = getByTestId('facet-entity-DATASET');
        expect(datasetEntityBox).toHaveProperty('checked', true);

        const chartEntityBox = getByTestId('facet-entity-CHART');
        expect(chartEntityBox).toHaveProperty('checked', false);
        act(() => {
            fireEvent.click(chartEntityBox);
        });

        await waitFor(() => expect(queryByTestId('facet-entity-DATASET')).toBeInTheDocument());

        const datasetEntityBox2 = getByTestId('facet-entity-DATASET');
        expect(datasetEntityBox2).toHaveProperty('checked', true);

        const chartEntityBox2 = getByTestId('facet-entity-CHART');
        expect(chartEntityBox2).toHaveProperty('checked', true);
        await act(() => promise);
    });
});
