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

import GlossaryRelatedTerms from '@app/entityV2/glossaryTerm/profile/GlossaryRelatedTerms';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

describe('Glossary Related Terms', () => {
    it('renders and print hasRelatedTerms detail by default', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryRelatedTerms />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('Contains')).toBeInTheDocument();
        expect(getByText('Inherits')).toBeInTheDocument();
    });
});
