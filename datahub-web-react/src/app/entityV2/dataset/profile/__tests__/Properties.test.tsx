import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import { sampleProperties } from '@app/entityV2/dataset/profile/stories/properties';
import { Properties } from '@app/entityV2/shared/components/legacy/Properties';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

describe('Properties', () => {
    it('renders', () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <Properties properties={sampleProperties} />,
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('Properties')).toBeInTheDocument();
        expect(getByText('Number of Partitions')).toBeInTheDocument();
        expect(getByText('18')).toBeInTheDocument();
        expect(getByText('Cluster Name')).toBeInTheDocument();
        expect(getByText('Testing')).toBeInTheDocument();
    });
});
