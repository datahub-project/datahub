import React from 'react';
import { fireEvent, render, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';

import { SearchPage } from '../SearchPage';
import TestPageContainer from '../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../Mocks';

describe('SearchPage', () => {
    it('renders', () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/search/dataset?filter_platform=hive,kafka&page=1&query=sample']}>
                    <SearchPage />
                </TestPageContainer>
            </MockedProvider>,
        );
    });

    it('renders loading', () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/search/dataset?filter_platform=hive,kafka&page=1&query=sample']}>
                    <SearchPage />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('Loading')).toBeInTheDocument();
    });

    it('renders the selected filters as checked', async () => {
        const { getByTestId, queryByTestId } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/search/dataset?filter_platform=kafka&page=1&query=test']}>
                    <SearchPage />
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
    });

    it('renders multiple checked filters at once', async () => {
        const { getByTestId, queryByTestId } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/search/dataset?filter_platform=kafka,hdfs&page=1&query=test']}>
                    <SearchPage />
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
    });

    it('clicking a filter selects a new filter', async () => {
        const { getByTestId, queryByTestId, queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/search/dataset?filter_platform=kafka&page=1&query=test']}>
                    <SearchPage />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByTestId('facet-platform-kafka')).toBeInTheDocument());

        const kafkaPlatformBox = getByTestId('facet-platform-kafka');
        expect(kafkaPlatformBox).toHaveProperty('checked', true);

        const hdfsPlatformBox = getByTestId('facet-platform-hdfs');
        expect(hdfsPlatformBox).toHaveProperty('checked', false);

        expect(queryByText('Loading')).not.toBeInTheDocument();
        fireEvent.click(hdfsPlatformBox);
        expect(queryByText('Loading')).toBeInTheDocument();

        await waitFor(() => expect(queryByTestId('facet-platform-kafka')).toBeInTheDocument());

        const kafkaPlatformBox2 = getByTestId('facet-platform-kafka');
        expect(kafkaPlatformBox2).toHaveProperty('checked', true);

        const hdfsPlatformBox2 = getByTestId('facet-platform-hdfs');
        expect(hdfsPlatformBox2).toHaveProperty('checked', true);
    });
});
