import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import Lineage from '@app/entity/dataset/profile/Lineage';
import { sampleDownstreamRelationship, sampleRelationship } from '@app/entity/dataset/profile/stories/lineageEntities';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

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
