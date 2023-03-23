import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';
import { mocks } from '../../../../../Mocks';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import GlossaryTermHeader from '../GlossaryTermHeader';

const glossaryTermHeaderData = {
    definition: 'this is sample definition',
    termSource: 'termSource',
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
                        termSource={glossaryTermHeaderData.termSource}
                        sourceRef={glossaryTermHeaderData.sourceRef}
                        fqdn={glossaryTermHeaderData.fqdn}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText(glossaryTermHeaderData.definition)).toBeInTheDocument();
    });
});
