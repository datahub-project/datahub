import React from 'react';
import { render } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import { Properties } from '../../../shared/components/legacy/Properties';
import { sampleProperties } from '../stories/properties';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../../../Mocks';

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
