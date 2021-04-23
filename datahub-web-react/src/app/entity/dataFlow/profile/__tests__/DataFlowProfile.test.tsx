import React from 'react';
import { render, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';

import { DataFlowProfile } from '../DataFlowProfile';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../../../Mocks';

describe('DataJobProfile', () => {
    it('renders', async () => {
        const { getByText, queryAllByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/pipeline/urn:li:dataFlow:1']}>
                    <DataFlowProfile urn="urn:li:dataFlow:1" />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(queryAllByText('DataFlowInfoName').length).toBeGreaterThanOrEqual(1));

        expect(getByText('DataFlowInfo1 Description')).toBeInTheDocument();
    });
});
