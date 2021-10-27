import { render } from '@testing-library/react';
import React from 'react';
import { MockedProvider } from '@apollo/client/testing';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import GlossaryRelatedTerms from '../GlossaryRelatedTerms';
import { mocks } from '../../../../../Mocks';

const glossaryRelatedTermData = {
    isRealtedTerms: {
        start: 0,
        count: 0,
        total: 0,
        relationships: [
            {
                entity: {
                    urn: 'urn:li:glossaryTerm:schema.Field16Schema_v1',
                    __typename: 'GlossaryTerm',
                },
            },
        ],
        __typename: 'EntityRelationshipsResult',
    },
    hasRelatedTerms: {
        start: 0,
        count: 0,
        total: 0,
        relationships: [
            {
                entity: {
                    urn: 'urn:li:glossaryTerm:example.glossaryterm2',
                    __typename: 'GlossaryTerm',
                },
            },
        ],
        __typename: 'EntityRelationshipsResult',
    },
};

describe('Glossary Related Terms', () => {
    it('renders and print hasRelatedTerms detail by default', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryRelatedTerms glossaryTerm={glossaryRelatedTermData} />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(getByText('Contains')).toBeInTheDocument();
        expect(getByText('Inherits')).toBeInTheDocument();
    });
});
