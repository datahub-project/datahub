import React from 'react';
import { render, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import { Route } from 'react-router';

import TagProfile from '../TagProfile';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../../Mocks';

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
        const { getByTestId, queryByText } = render(
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

        await waitFor(() => expect(queryByText('abc-sample-tag')).toBeInTheDocument());

        expect(getByTestId('avatar-tag-urn:li:corpuser:3')).toBeInTheDocument();
        expect(getByTestId('avatar-tag-urn:li:corpuser:1')).toBeInTheDocument();
    });

    it('renders stats', async () => {
        const { getByTestId, queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/tag/urn:li:tag:abc-sample-tag']}>
                    <Route path="/tag/:urn" render={() => <TagProfile />} />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByText('abc-sample-tag')).toBeInTheDocument());

        await waitFor(() => expect(queryByText('Loading')).not.toBeInTheDocument());

        expect(getByTestId('stats-DATASET')).toBeInTheDocument();
        expect(getByTestId('stats-CORP_USER')).toBeInTheDocument();
        expect(queryByText('1 Datasets')).toBeInTheDocument();
        expect(queryByText('2 Users')).toBeInTheDocument();
    });
});
