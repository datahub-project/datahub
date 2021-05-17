import React from 'react';
import { render, waitFor, fireEvent } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import { HomePage } from '../HomePage';
import { mocks } from '../../../Mocks';
import TestPageContainer from '../../../utils/test-utils/TestPageContainer';

describe('HomePage', () => {
    it('renders', () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
    });

    it('renders browsable entities', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(getByText('Datasets')).toBeInTheDocument());
    });

    it('renders autocomplete results', async () => {
        const { getByTestId, queryByTitle } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
        const searchInput = getByTestId('search-input');
        fireEvent.change(searchInput, { target: { value: 't' } });

        await waitFor(() => expect(queryByTitle('The Great Test Dataset')).toBeInTheDocument());
        await waitFor(() => expect(queryByTitle('Some other test')).toBeInTheDocument());
    });

    it('renders search suggestions', async () => {
        const { getByText, queryAllByText } = render(
            <MockedProvider mocks={mocks} addTypename>
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(getByText('Try searching for...')).toBeInTheDocument());
        expect(queryAllByText('Yet Another Dataset').length).toBeGreaterThanOrEqual(1);
        expect(queryAllByText('Fourth Test Dataset').length).toBeGreaterThanOrEqual(1);
    });
});
