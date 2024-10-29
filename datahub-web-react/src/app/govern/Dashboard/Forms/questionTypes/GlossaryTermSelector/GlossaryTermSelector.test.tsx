import React from 'react';
import { render, waitFor, fireEvent, screen } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import GlossaryTermsSelector from '../GlossaryTermsSelector';
import TestPageContainer from '@src/utils/test-utils/TestPageContainer';
import { GetSearchResultsForMultipleDocument } from '@src/graphql/search.generated';
import { InMemoryCache } from '@apollo/client';
import { EntityType } from '@src/types.generated';

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
                searchAcrossEntities: {
                    start: 0,
                    count: 5,
                    total: 21,
                    searchResults: [
                        {
                            entity: {
                                urn: 'urn:li:glossaryNode:4234668a-8e3c-42a9-91f3-93592de52c01',
                                type: EntityType.GlossaryNode,
                                properties: {
                                    name: 'Growth',
                                    description:
                                        'Welcome to the Growth Metrics Collection! This repository contains a comprehensive list of growth-related metric definitions and explanations. These metrics are essential for businesses, startups, and organizations looking to measure and track their growth and performance over time. Whether you are a marketer, a product manager, or an analyst, this collection will provide you with a valuable resource to understand, calculate, and interpret key growth metrics.',
                                    __typename: 'GlossaryNodeProperties',
                                },
                                displayProperties: null,
                                __typename: 'GlossaryNode',
                                parentNodes: {
                                    count: 0,
                                    nodes: [],
                                    __typename: 'ParentNodesResult',
                                },
                            },
                            matchedFields: [
                                {
                                    name: 'hasParentNode',
                                    value: '',
                                    entity: null,
                                    __typename: 'MatchedField',
                                },
                            ],
                            insights: [],
                            extraProperties: [
                                {
                                    name: 'urn',
                                    value: '"urn:li:glossaryNode:ClientsAndAccounts"',
                                    __typename: 'ExtraProperty',
                                },
                            ],
                            __typename: 'SearchResult',
                        },
                    ],
                    facets: [],
                    suggestions: [],
                    __typename: 'SearchResults',
                },
            },
        },
    },
];

describe('GlossaryTermsSelector', () => {
    const initialOptions = [
        {
            value: 'urn:li:glossaryTerm:ac4728586e658ed7c0977969e12d09e1',
            label: 'AccountType',
            id: 'urn:li:glossaryTerm:ac4728586e658ed7c0977969e12d09e1',
            isParent: false,
            parentId: 'urn:li:glossaryNode:6f63035e8a24155de46020549e9fd075',
            entity: {
                urn: 'urn:li:glossaryTerm:ac4728586e658ed7c0977969e12d09e1',
                type: 'GLOSSARY_TERM',
                name: 'ac4728586e658ed7c0977969e12d09e1',
                hierarchicalName: 'ac4728586e658ed7c0977969e12d09e1',
                properties: {
                    name: 'AccountType',
                    __typename: 'GlossaryTermProperties',
                },
                parentNodes: {
                    nodes: [
                        {
                            urn: 'urn:li:glossaryNode:6f63035e8a24155de46020549e9fd075',
                            __typename: 'GlossaryNode',
                        },
                    ],
                    __typename: 'ParentNodesResult',
                },
                __typename: 'GlossaryTerm',
            },
        },
        {
            value: 'urn:li:glossaryTerm:37c7edbc0b5129ab6b4da4021dfed7d3',
            label: 'Currency',
            id: 'urn:li:glossaryTerm:37c7edbc0b5129ab6b4da4021dfed7d3',
            isParent: false,
            parentId: 'urn:li:glossaryNode:6f63035e8a24155de46020549e9fd075',
            entity: {
                urn: 'urn:li:glossaryTerm:37c7edbc0b5129ab6b4da4021dfed7d3',
                type: 'GLOSSARY_TERM',
                name: '37c7edbc0b5129ab6b4da4021dfed7d3',
                hierarchicalName: '37c7edbc0b5129ab6b4da4021dfed7d3',
                properties: {
                    name: 'Currency',
                    __typename: 'GlossaryTermProperties',
                },
                parentNodes: {
                    nodes: [
                        {
                            urn: 'urn:li:glossaryNode:6f63035e8a24155de46020549e9fd075',
                            __typename: 'GlossaryNode',
                        },
                    ],
                    __typename: 'ParentNodesResult',
                },
                __typename: 'GlossaryTerm',
            },
        },

        {
            value: 'urn:li:glossaryTerm:Classification.Sensitive',
            label: 'Sensitive',
            id: 'urn:li:glossaryTerm:Classification.Sensitive',
            isParent: false,
            entity: {
                type: 'GLOSSARY_TERM',
                urn: 'urn:li:glossaryTerm:Classification.Sensitive',
                name: 'Sensitive',
                hierarchicalName: 'Classification.Sensitive',
                properties: {
                    name: 'Sensitive',
                    description: 'Sensitive Data',
                    __typename: 'GlossaryTermProperties',
                },
                __typename: 'GlossaryTerm',
            },
        },
    ];
    it('renders GlossaryTermsSelector without none select', async () => {
        const { getByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
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
    it.skip('show number of selected pills in glossary term selector', async () => {
        const { getByText, getAllByTestId } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryTermsSelector
                        initialOptions={initialOptions}
                        label="Glossary Terms"
                        placeholder="Select allowed glossary terms"
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        const selectedGlossaryItemPills = getAllByTestId('pill-container');
        screen.debug();

        await waitFor(() => expect(getByText('Glossary Terms')).toBeInTheDocument());
        await waitFor(() => expect(selectedGlossaryItemPills.length).toBe(initialOptions.length));
    });
    it.skip('check selected glossary term clear all the selected items ', async () => {
        const { getByTestId, getAllByTestId } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryTermsSelector
                        initialOptions={initialOptions}
                        label="Glossary Terms"
                        placeholder="Select allowed glossary terms"
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        const selectedGlossaryItemPills = getAllByTestId('pill-container');
        await waitFor(() => expect(selectedGlossaryItemPills.length).toBe(initialOptions.length));

        const dropdownClearIcon = getByTestId('dropdown-option-clear-icon');
        fireEvent.click(dropdownClearIcon);
        const selectedItemPill = screen.queryByTestId('pill-container');
        expect(selectedItemPill).not.toBeInTheDocument();
    });

    it.skip('check selected glossary term showing when user clicks on dropdown ', async () => {
        const { getByTestId, getAllByTestId } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <GlossaryTermsSelector
                        initialOptions={initialOptions}
                        label="Glossary Terms"
                        placeholder="Select allowed glossary terms"
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        // screen.debug();
        const dropdownContainer = getByTestId('nested-options-dropdown-container');
        fireEvent.click(dropdownContainer);
        screen.debug();
        const selectedGlossaryItemPills = getAllByTestId('pill-container');
        // await waitFor(() => expect(selectedGlossaryItemPills.length).toBe(initialOptions.length));

        // const dropdownClearIcon = getByTestId('dropdown-option-clear-icon');
        // fireEvent.click(dropdownClearIcon);
        // const selectedItemPill = screen.queryByTestId('pill-container');
        // expect(selectedItemPill).not.toBeInTheDocument();
    });

    // const mockOnSubmit = vi.fn(); // Create a mock function
});
