/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { MockedProvider } from '@apollo/client/testing';
import { render, waitFor } from '@testing-library/react';
import React from 'react';

import { Routes } from '@app/Routes';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

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
