/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import Lineage from '@app/entityV2/dataset/profile/Lineage';
import {
    sampleDownstreamRelationship,
    sampleRelationship,
} from '@app/entityV2/dataset/profile/stories/lineageEntities';
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
