import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';
import { mocks } from '../../../../../Mocks';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import GlossaryTermHeader from '../GlossaryTermHeader';

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
