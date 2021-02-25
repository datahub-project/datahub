import React from 'react';
import { fireEvent, render, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import { Route } from 'react-router';

import { SearchPage } from '../SearchPage';
import TestPageContainer from '../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../Mocks';
import { PageRoutes } from '../../../conf/Global';

describe('SearchPage', () => {
    it('renders', () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/search/dataset?filter_platform=hive,kafka&page=1&query=sample']}>
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );
    });

    it('renders loading', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/search/dataset?filter_platform=hive,kafka&page=1&query=sample']}>
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(getByText('Loading...')).toBeInTheDocument());
    });

    it('renders the selected filters as checked', async () => {
        const { getByTestId, queryByTestId } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/search/dataset?filter_platform=kafka&page=1&query=test']}>
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(getByTestId('filters-button')).toBeInTheDocument());
        const filtersButton = getByTestId('filters-button');
        fireEvent.click(filtersButton);

        await waitFor(() => expect(queryByTestId('facet-platform-kafka')).toBeInTheDocument());

        const kafkaPlatformBox = getByTestId('facet-platform-kafka');
        expect(kafkaPlatformBox).toHaveProperty('checked', true);

        const hdfsPlatformBox = getByTestId('facet-platform-hdfs');
        expect(hdfsPlatformBox).toHaveProperty('checked', false);

        const prodOriginBox = getByTestId('facet-origin-PROD');
        expect(prodOriginBox).toHaveProperty('checked', false);
    });

    it('renders multiple checked filters at once', async () => {
        const { getByTestId, queryByTestId } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/search/dataset?filter_platform=kafka,hdfs&page=1&query=test']}>
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(getByTestId('filters-button')).toBeInTheDocument());
        const filtersButton = getByTestId('filters-button');
        fireEvent.click(filtersButton);

        await waitFor(() => expect(queryByTestId('facet-platform-kafka')).toBeInTheDocument());

        const kafkaPlatformBox = getByTestId('facet-platform-kafka');
        expect(kafkaPlatformBox).toHaveProperty('checked', true);

        const hdfsPlatformBox = getByTestId('facet-platform-hdfs');
        expect(hdfsPlatformBox).toHaveProperty('checked', true);

        const prodOriginBox = getByTestId('facet-origin-PROD');
        expect(prodOriginBox).toHaveProperty('checked', false);
    });

    it('clicking a filter selects a new filter', async () => {
        const { getByTestId, queryByTestId } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/search/dataset?filter_platform=kafka&page=1&query=test']}>
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(getByTestId('filters-button')).toBeInTheDocument());
        const filtersButton = getByTestId('filters-button');
        fireEvent.click(filtersButton);

        await waitFor(() => expect(queryByTestId('facet-platform-kafka')).toBeInTheDocument());

        const kafkaPlatformBox = getByTestId('facet-platform-kafka');
        expect(kafkaPlatformBox).toHaveProperty('checked', true);

        const hdfsPlatformBox = getByTestId('facet-platform-hdfs');
        expect(hdfsPlatformBox).toHaveProperty('checked', false);

        fireEvent.click(hdfsPlatformBox);

        await waitFor(() => expect(queryByTestId('facet-platform-kafka')).toBeInTheDocument());

        const kafkaPlatformBox2 = getByTestId('facet-platform-kafka');
        expect(kafkaPlatformBox2).toHaveProperty('checked', true);

        const hdfsPlatformBox2 = getByTestId('facet-platform-hdfs');
        expect(hdfsPlatformBox2).toHaveProperty('checked', true);
    });
});
