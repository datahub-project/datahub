import React from 'react';
import { render } from '@testing-library/react';
import Lineage from '../Lineage';
import { sampleDownstreamRelationship, sampleRelationship } from '../stories/lineageEntities';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';

describe('Lineage', () => {
    it('renders', () => {
        const { getByText } = render(
            <TestPageContainer>
                <Lineage upstreamLineage={sampleRelationship} downstreamLineage={sampleDownstreamRelationship} />,
            </TestPageContainer>,
        );
        expect(getByText('Upstream HiveDataset')).toBeInTheDocument();
        expect(getByText('Downstream HiveDataset')).toBeInTheDocument();
    });
});
