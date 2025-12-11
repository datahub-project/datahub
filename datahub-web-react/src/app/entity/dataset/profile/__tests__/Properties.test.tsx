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

import { sampleProperties } from '@app/entity/dataset/profile/stories/properties';
import { Properties } from '@app/entity/shared/components/legacy/Properties';
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
