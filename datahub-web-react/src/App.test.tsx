import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { InnerApp } from '@src/App';
import { mocks } from '@src/Mocks';

// eslint-disable-next-line vitest/expect-expect
test('renders the app', async () => {
    render(
        <MockedProvider mocks={mocks} addTypename={false}>
            <InnerApp />
        </MockedProvider>,
    );
});
