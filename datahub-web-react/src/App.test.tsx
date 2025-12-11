/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
