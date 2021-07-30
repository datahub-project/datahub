import React from 'react';
import { render, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';

import { MLModelGroupProfile } from '../profile/MLModelGroupProfile';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../../Mocks';

describe('MlModelGroupProfile', () => {
    it('renders', async () => {
        const { getByText, queryAllByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer
                    initialEntries={[
                        '/mlModelGroup/urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,another-group,PROD)',
                    ]}
                >
                    <MLModelGroupProfile urn="urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,another-group,PROD)" />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(queryAllByText('trust model group').length).toBeGreaterThanOrEqual(1));

        expect(getByText('a ml trust model group')).toBeInTheDocument();
    });
});
