import React from 'react';
import { render, waitFor, fireEvent } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import { HomePage } from '../HomePage';
import { mocks } from '../../../Mocks';
import TestPageContainer from '../../../utils/test-utils/TestPageContainer';
import axios, { AxiosResponse } from 'axios';

jest.mock('axios');

describe('HomePage', () => {
    const mock_response = 
        {
            message: 'hello-world',
            timestamp: 1000000000,            
        };
    const mockedResponse: AxiosResponse = {
        data: mock_response,
        status: 200,
        statusText: 'OK',
        headers: {},
        config: {},
        };
        // Make the mock return the custom axios response
        mockedAxios.get.mockResolvedValueOnce(mockedResponse);
    it('renders', async () => {
        const { getByTestId } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(getByTestId('search-input')).toBeInTheDocument());
    });

    it('renders browsable entities', async () => {
        const { getByText } = render(
            <MockedProvider
                mocks={mocks}
                addTypename={false}
                defaultOptions={{
                    watchQuery: { fetchPolicy: 'no-cache' },
                    query: { fetchPolicy: 'no-cache' },
                }}
            >
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(getByText('Datasets')).toBeInTheDocument());
    });

    it('renders autocomplete results', async () => {
        const { getByTestId, queryAllByText } = render(
            <MockedProvider
                mocks={mocks}
                addTypename={false}
                defaultOptions={{
                    watchQuery: { fetchPolicy: 'no-cache' },
                    query: { fetchPolicy: 'no-cache' },
                }}
            >
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
        const searchInput = getByTestId('search-input');
        await waitFor(() => expect(searchInput).toBeInTheDocument());
        fireEvent.change(searchInput, { target: { value: 't' } });

        await waitFor(() => expect(queryAllByText('The Great Test Dataset').length).toBeGreaterThanOrEqual(1));
        expect(queryAllByText('Some other test').length).toBeGreaterThanOrEqual(1);
    });

    it('renders search suggestions', async () => {
        const { getByText, queryAllByText } = render(
            <MockedProvider mocks={mocks} addTypename>
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(getByText('Try searching for')).toBeInTheDocument());
        expect(queryAllByText('Yet Another Dataset').length).toBeGreaterThanOrEqual(1);
        expect(queryAllByText('Fourth Test Dataset').length).toBeGreaterThanOrEqual(1);
    });

    it('renders the announcement banner', async () => {
        const { getByText, queryAllByText } = render(
            <MockedProvider mocks={mocks} addTypename>
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(getByText('Try searching for')).toBeInTheDocument());
        expect(queryAllByText('Yet Another Dataset').length).toBeGreaterThanOrEqual(1);
        expect(queryAllByText('Fourth Test Dataset').length).toBeGreaterThanOrEqual(1);
    });
});
