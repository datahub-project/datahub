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

        await waitFor(() => expect(queryByTestId('facet-platform-kafka')).toBeInTheDocument());

        const kafkaPlatformBox = getByTestId('facet-platform-kafka');
        expect(kafkaPlatformBox).toHaveProperty('checked', true);

        const hdfsPlatformBox = getByTestId('facet-platform-hdfs');
        expect(hdfsPlatformBox).toHaveProperty('checked', false);

        const prodOriginBox = getByTestId('facet-origin-PROD');
        expect(prodOriginBox).toHaveProperty('checked', false);
        await act(() => promise);
    });

    it('renders multiple checked filters at once', async () => {
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
                    initialEntries={['/search?filter_entity=DATASET&filter_platform=kafka,hdfs&page=1&query=test']}
                >
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByTestId('facet-platform-kafka')).toBeInTheDocument());

        const kafkaPlatformBox = getByTestId('facet-platform-kafka');
        expect(kafkaPlatformBox).toHaveProperty('checked', true);

        const hdfsPlatformBox = getByTestId('facet-platform-hdfs');
        expect(hdfsPlatformBox).toHaveProperty('checked', true);

        const prodOriginBox = getByTestId('facet-origin-PROD');
        expect(prodOriginBox).toHaveProperty('checked', false);
        await act(() => promise);
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

        await waitFor(() => expect(queryByTestId('facet-platform-kafka')).toBeInTheDocument());

        const kafkaPlatformBox = getByTestId('facet-platform-kafka');
        expect(kafkaPlatformBox).toHaveProperty('checked', true);

        const hdfsPlatformBox = getByTestId('facet-platform-hdfs');
        expect(hdfsPlatformBox).toHaveProperty('checked', false);
        act(() => {
            fireEvent.click(hdfsPlatformBox);
        });

        await waitFor(() => expect(queryByTestId('facet-platform-kafka')).toBeInTheDocument());

        const kafkaPlatformBox2 = getByTestId('facet-platform-kafka');
        expect(kafkaPlatformBox2).toHaveProperty('checked', true);

        const hdfsPlatformBox2 = getByTestId('facet-platform-hdfs');
        expect(hdfsPlatformBox2).toHaveProperty('checked', true);
        await act(() => promise);
    });
});
