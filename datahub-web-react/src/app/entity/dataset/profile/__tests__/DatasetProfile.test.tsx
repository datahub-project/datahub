import React from 'react';
import { render, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';

import { DatasetProfile } from '../DatasetProfile';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../../../Mocks';

describe('DatasetProfile', () => {
    it('renders', () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <DatasetProfile urn="urn:li:dataset:3" />
                </TestPageContainer>
            </MockedProvider>,
        );
    });

    it('renders tags', async () => {
        const { getByText, queryByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
                    <DatasetProfile urn="urn:li:dataset:3" />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryByText('abc-sample-tag')).toBeInTheDocument());

        expect(getByText('abc-sample-tag')).toBeInTheDocument();
        expect(getByText('abc-sample-tag').closest('a').href).toEqual('http://localhost/tag/urn:li:tag:abc-sample-tag');
    });
});
