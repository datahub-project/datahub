import React from 'react';
import { render, waitFor, fireEvent, screen } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import GlossaryTermsSelector from '../GlossaryTermsSelector';
import TestPageContainer from '@src/utils/test-utils/TestPageContainer';
import { GetSearchResultsForMultipleDocument } from '@src/graphql/search.generated';

const mocks = [
    {
        request: {
            query: GetSearchResultsForMultipleDocument,
            variables: {
                input: {
                    query: '*',
                    types: ['GLOSSARY_NODE', 'GLOSSARY_TERM'],
                    orFilters: [
                        {
                            and: [
                                {
                                    field: 'hasParentNode',
                                    value: 'false',
                                },
                            ],
                        },
                    ],
                },
            },
        },
        result: {
            data: {
                __typename: 'Query',
                searchAcrossEntities: {
                    start: 0,
                    count: 2,
                    total: 2,
                    searchResults: [],
                    facets: [],
                    suggestions: [],
                },
            },
            extensions: {},
        },
    },
];

describe('GlossaryTermsSelector', () => {
    it('renders GlossaryTermsSelector without none select', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={true}>
                <TestPageContainer>
                    <GlossaryTermsSelector
                        initialOptions={[]}
                        label="Glossary Terms"
                        placeholder="Select allowed glossary terms"
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(getByText('Glossary Terms')).toBeInTheDocument());
        await waitFor(() => expect(getByText('Select allowed glossary terms')).toBeInTheDocument());
    });
});
