import React from 'react';
import { render, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';

import { DataJobProfile } from '../DataJobProfile';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../../../Mocks';

describe('DataJobProfile', () => {
    it('renders', async () => {
        const { getByText, queryAllByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/datajob/urn:li:dataJob:1']}>
                    <DataJobProfile urn="urn:li:dataJob:1" />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(queryAllByText('DataJobInfoName').length).toBeGreaterThanOrEqual(1));

        expect(getByText('DataJobInfo1 Description')).toBeInTheDocument();
    });
});
