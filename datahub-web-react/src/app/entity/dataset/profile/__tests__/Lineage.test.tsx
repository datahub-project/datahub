import React from 'react';
import { render } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';

import Lineage from '../Lineage';
import { sampleDownstreamRelationship, sampleRelationship } from '../stories/lineageEntities';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../../../Mocks';

describe('Lineage', () => {
    it('renders', () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <Lineage upstreamLineage={sampleRelationship} downstreamLineage={sampleDownstreamRelationship} />,
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('Upstream HiveDataset')).toBeInTheDocument();
        expect(getByText('Downstream HiveDataset')).toBeInTheDocument();
    });
});
