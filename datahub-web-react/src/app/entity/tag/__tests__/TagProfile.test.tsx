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
import { Route } from 'react-router';

import TagProfile from '@app/entity/tag/TagProfile';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

describe('TagProfile', () => {
    it('renders tag details', async () => {
        const { getByText, queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/tag/urn:li:tag:abc-sample-tag']}>
                    <Route path="/tag/:urn" render={() => <TagProfile />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByText('abc-sample-tag')).toBeInTheDocument());

        expect(getByText('abc-sample-tag')).toBeInTheDocument();
        expect(getByText('sample tag description')).toBeInTheDocument();
    });

    it('renders tag ownership', async () => {
        const { queryByText } = render(
            <MockedProvider
                mocks={mocks}
                addTypename={false}
                defaultOptions={{
                    watchQuery: { fetchPolicy: 'no-cache' },
                    query: { fetchPolicy: 'no-cache' },
                }}
            >
                <TestPageContainer initialEntries={['/tag/urn:li:tag:abc-sample-tag']}>
                    <Route path="/tag/:urn" render={() => <TagProfile />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByText('abc-sample-tag')).not.toBeInTheDocument());
    });

    it('renders stats', async () => {
        const { queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/tag/urn:li:tag:abc-sample-tag']}>
                    <Route path="/tag/:urn" render={() => <TagProfile />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByText('abc-sample-tag')).not.toBeInTheDocument());

        await waitFor(() => expect(queryByText('Loading')).not.toBeInTheDocument());
    });
});
