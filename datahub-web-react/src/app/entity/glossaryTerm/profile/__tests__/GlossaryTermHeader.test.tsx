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

import GlossaryTermHeader from '@app/entity/glossaryTerm/profile/GlossaryTermHeader';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const glossaryTermHeaderData = {
    definition: 'this is sample definition',
    sourceUrl: 'sourceUrl',
    sourceRef: 'Source ref',
    fqdn: 'fqdn',
};

describe('Glossary Term Header', () => {
    it('renders', () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks}>
                <TestPageContainer>
                    <GlossaryTermHeader
                        definition={glossaryTermHeaderData.definition}
                        sourceUrl={glossaryTermHeaderData.sourceUrl}
                        sourceRef={glossaryTermHeaderData.sourceRef}
                        ownership={undefined}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText(glossaryTermHeaderData.definition)).toBeInTheDocument();
    });
});
