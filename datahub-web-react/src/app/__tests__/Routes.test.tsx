import React from 'react';
import { render, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import { mocks } from '../../Mocks';
import TestPageContainer from '../../utils/test-utils/TestPageContainer';
import { Routes } from '../Routes';

test('renders embed page properly', async () => {
    const { getByText } = render(
        <MockedProvider mocks={mocks} addTypename={false}>
            <TestPageContainer initialEntries={['/embed/dataset/urn:li:dataset:3']}>
                <Routes />
            </TestPageContainer>
        </MockedProvider>,
    );

    await waitFor(() => expect(getByText('Yet Another Dataset')).toBeInTheDocument());
});
