import React from 'react';
import { render, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';

import { MLModelProfile } from '../profile/MLModelProfile';
import TestPageContainer from '../../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../../Mocks';

describe('MlModelProfile', () => {
    it('renders', async () => {
        const { getByText, queryAllByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer
                    initialEntries={['/mlModel/urn:li:mlModel:(urn:li:dataPlatform:sagemaker,trustmodel,PROD)']}
                >
                    <MLModelProfile urn="urn:li:mlModel:(urn:li:dataPlatform:sagemaker,trustmodel,PROD)" />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(queryAllByText('trust model').length).toBeGreaterThanOrEqual(1));

        expect(getByText('a ml trust model')).toBeInTheDocument();
    });
});
