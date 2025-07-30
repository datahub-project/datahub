import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';

import GlossaryTermHeader from '@app/entityV2/glossaryTerm/profile/GlossaryTermHeader';
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
