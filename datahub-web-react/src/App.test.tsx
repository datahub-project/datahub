import React from 'react';
import { render } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import { InnerApp } from './App';
import { mocks } from './Mocks';

// eslint-disable-next-line vitest/expect-expect
test('renders the app', async () => {
    render(
        <MockedProvider mocks={mocks} addTypename={false}>
            <InnerApp />
        </MockedProvider>,
    );
});
