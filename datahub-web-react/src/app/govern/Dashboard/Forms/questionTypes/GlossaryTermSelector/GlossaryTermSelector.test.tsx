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
                                children: {
                                    total: 2,
                                    relationships: [
                                        {
                                            type: 'IsPartOf',
                                            entity: {
                                                urn: 'urn:li:glossaryTerm:9af98121-070c-40b0-8830-19ae6d2b4ac4',
                                                name: '9af98121-070c-40b0-8830-19ae6d2b4ac4',
                                                type: 'GLOSSARY_TERM',
                                                hierarchicalName: '9af98121-070c-40b0-8830-19ae6d2b4ac4',
                                                properties: {
                                                    name: 'Lifetime Purchase Count',
                                                    description:
                                                        '**CLPC = (Gross Count) - (Cancellations + Chargebacks + Fraud Detected Transactions)**',
                                                    __typename: 'GlossaryTermProperties',
                                                },
                                                __typename: 'GlossaryTerm',
                                            },
                                            __typename: 'EntityRelationship',
                                        },
                                        {
                                            type: 'IsPartOf',
                                            entity: {
                                                urn: 'urn:li:glossaryTerm:4c83d4ea-256d-48a7-aa79-73bc4aa6c97c',
                                                name: '4c83d4ea-256d-48a7-aa79-73bc4aa6c97c',
                                                type: 'GLOSSARY_TERM',
                                                hierarchicalName: '4c83d4ea-256d-48a7-aa79-73bc4aa6c97c',
                                                properties: {
                                                    name: 'Lifetime Purchase Amount',
                                                    description:
                                                        "Lifetime purchase amount is calculated off of the following formula, Make a change\n\n&nbsp;\n\n**CLTV = Gross Margin - (CAC + M&S + SC + Cancellations)**\n\nWhere:\n\n*   **Gross Margin**: The profit earned from a customer over their lifetime, accounting for product costs, refunds, and chargebacks.\n    \n    &nbsp;\n    \n*   **CAC**: Customer Acquisition Cost, which includes all costs associated with acquiring a customer, including marketing expenses, sales commissions, and any promotional costs.\n    \n*   **M&S**: Marketing and Sales expenses incurred over the customer's lifetime.\n    \n*   **SC**: Support Costs, including customer service and support expenses.\n    \n*   **Cancellations:** All orders that were cancelled, charged-back, or detected as fraud\n    \n\nHere's how to calculate each component:\n\n1.  **Gross Margin**:\n    \n    *   Calculate the total revenue generated from the customer over their lifetime, including all purchases and subscriptions.\n        \n    *   Subtract the total cost of goods sold (COGS), refunds, and chargebacks associated with those revenues.\n        \n    *   Divide the result by the number of customers to get the average gross margin per customer.\n        \n    \n    Example: Gross Margin = (Total Revenue - Total COGS - Total Refunds - Total Chargebacks) / Number of Customers\n    \n2.  **CAC**:\n    \n    *   Calculate the total costs associated with acquiring customers. Include marketing expenses, promotional costs, and sales commissions.\n        \n    *   Divide this by the number of customers acquired during the same period.\n        \n    \n    Example: CAC = (Total Marketing Expenses + Total Promotional Costs + Total Sales Commissions) / Number of Customers Acquired\n    \n3.  **M&S**:\n    \n    *   Calculate the total marketing and sales expenses incurred over the customer's lifetime. This can include ongoing marketing campaigns and sales efforts.\n        \n    *   Divide this by the number of customers.\n        \n    \n    Example: M&S = (Total Marketing Expenses + Total Sales Expenses) / Number of Customers\n    \n4.  **SC**:\n    \n    *   Calculate the total support costs, including customer service and support expenses, incurred over the customer's lifetime.\n        \n    *   Divide this by the number of customers.\n        \n    \n    Example: SC = (Total Support Expenses) / Number of Customers",
                                                    __typename: 'GlossaryTermProperties',
                                                },
                                                __typename: 'GlossaryTerm',
                                            },
                                            __typename: 'EntityRelationship',
                                        },
                                    ],
                                    __typename: 'EntityRelationshipsResult',
                                },
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
                    facets: [
                        {
                            field: '_entityType␞typeNames',
                            displayName: 'Type␞null',
                            entity: null,
                            aggregations: [
                                {
                                    value: 'GLOSSARY_NODE',
                                    count: 17,
                                    entity: null,
                                    __typename: 'AggregationMetadata',
                                },
                                {
                                    value: 'GLOSSARY_TERM',
                                    count: 4,
                                    entity: null,
                                    __typename: 'AggregationMetadata',
                                },
                            ],
                            __typename: 'FacetMetadata',
                        },
                        {
                            field: 'owners',
                            displayName: 'Owned By',
                            entity: null,
                            aggregations: [
                                {
                                    value: 'urn:li:corpuser:admin',
                                    count: 7,
                                    entity: {
                                        urn: 'urn:li:corpuser:admin',
                                        type: 'CORP_USER',
                                        username: 'admin',
                                        properties: {
                                            displayName: 'Admin',
                                            title: 'DataHub Root User',
                                            firstName: null,
                                            lastName: null,
                                            fullName: null,
                                            email: null,
                                            __typename: 'CorpUserProperties',
                                        },
                                        info: {
                                            active: true,
                                            displayName: 'Admin',
                                            title: 'DataHub Root User',
                                            firstName: null,
                                            lastName: null,
                                            fullName: null,
                                            email: null,
                                            __typename: 'CorpUserInfo',
                                        },
                                        __typename: 'CorpUser',
                                        editableProperties: {
                                            displayName: 'DataHub Administrator',
                                            pictureLink: '',
                                            __typename: 'CorpUserEditableProperties',
                                        },
                                    },
                                    __typename: 'AggregationMetadata',
                                },
                                {
                                    value: 'urn:li:corpuser:gabe@acryl.io',
                                    count: 4,
                                    entity: {
                                        urn: 'urn:li:corpuser:gabe@acryl.io',
                                        type: 'CORP_USER',
                                        username: 'gabe@acryl.io',
                                        properties: {
                                            displayName: 'Gabriel Lyons',
                                            title: null,
                                            firstName: 'Gabriel',
                                            lastName: 'Lyons',
                                            fullName: 'Gabriel Lyons',
                                            email: 'gabe@acryl.io',
                                            __typename: 'CorpUserProperties',
                                        },
                                        info: {
                                            active: true,
                                            displayName: 'Gabriel Lyons',
                                            title: null,
                                            firstName: 'Gabriel',
                                            lastName: 'Lyons',
                                            fullName: 'Gabriel Lyons',
                                            email: 'gabe@acryl.io',
                                            __typename: 'CorpUserInfo',
                                        },
                                        __typename: 'CorpUser',
                                        editableProperties: {
                                            displayName: 'Gabriel Lyons',
                                            pictureLink:
                                                'https://secure.gravatar.com/avatar/e904cb49384bb3a33075727b4f86dfc4.jpg?s=192&d=https%3A%2F%2Fa.slack-edge.com%2Fdf10d%2Fimg%2Favatars%2Fava_0023-192.png',
                                            __typename: 'CorpUserEditableProperties',
                                        },
                                    },
                                    __typename: 'AggregationMetadata',
                                },
                                {
                                    value: 'urn:li:corpGroup:finance',
                                    count: 1,
                                    entity: {
                                        urn: 'urn:li:corpGroup:finance',
                                        type: 'CORP_GROUP',
                                        name: 'finance',
                                        info: {
                                            displayName: 'Finance',
                                            __typename: 'CorpGroupInfo',
                                        },
                                        __typename: 'CorpGroup',
                                        properties: {
                                            displayName: 'Finance',
                                            __typename: 'CorpGroupProperties',
                                        },
                                    },
                                    __typename: 'AggregationMetadata',
                                },
                                {
                                    value: 'urn:li:corpuser:jdoe',
                                    count: 1,
                                    entity: {
                                        urn: 'urn:li:corpuser:jdoe',
                                        type: 'CORP_USER',
                                        username: 'jdoe',
                                        properties: {
                                            displayName: 'John Doe',
                                            title: 'Software Engineer',
                                            firstName: null,
                                            lastName: null,
                                            fullName: 'John Doe',
                                            email: 'jdoe@linkedin.com',
                                            __typename: 'CorpUserProperties',
                                        },
                                        info: {
                                            active: true,
                                            displayName: 'John Doe',
                                            title: 'Software Engineer',
                                            firstName: null,
                                            lastName: null,
                                            fullName: 'John Doe',
                                            email: 'jdoe@linkedin.com',
                                            __typename: 'CorpUserInfo',
                                        },
                                        __typename: 'CorpUser',
                                        editableProperties: null,
                                    },
                                    __typename: 'AggregationMetadata',
                                },
                                {
                                    value: 'urn:li:corpuser:maggiem.hays@gmail.com',
                                    count: 1,
                                    entity: {
                                        urn: 'urn:li:corpuser:maggiem.hays@gmail.com',
                                        type: 'CORP_USER',
                                        username: 'maggiem.hays@gmail.com',
                                        properties: {
                                            displayName: 'Maggie  - Admin to Test 2.0',
                                            title: 'Product Manager',
                                            firstName: null,
                                            lastName: null,
                                            fullName: 'Maggie  - Admin to Test 2.0',
                                            email: 'maggiem.hays@gmail.com',
                                            __typename: 'CorpUserProperties',
                                        },
                                        info: {
                                            active: true,
                                            displayName: 'Maggie  - Admin to Test 2.0',
                                            title: 'Product Manager',
                                            firstName: null,
                                            lastName: null,
                                            fullName: 'Maggie  - Admin to Test 2.0',
                                            email: 'maggiem.hays@gmail.com',
                                            __typename: 'CorpUserInfo',
                                        },
                                        __typename: 'CorpUser',
                                        editableProperties: null,
                                    },
                                    __typename: 'AggregationMetadata',
                                },
                                {
                                    value: 'urn:li:corpuser:mjames',
                                    count: 1,
                                    entity: {
                                        urn: 'urn:li:corpuser:mjames',
                                        type: 'CORP_USER',
                                        username: 'mjames',
                                        properties: null,
                                        info: null,
                                        __typename: 'CorpUser',
                                        editableProperties: null,
                                    },
                                    __typename: 'AggregationMetadata',
                                },
                                {
                                    value: 'urn:li:corpuser:nonie@longtail.com',
                                    count: 1,
                                    entity: {
                                        urn: 'urn:li:corpuser:nonie@longtail.com',
                                        type: 'CORP_USER',
                                        username: 'nonie@longtail.com',
                                        properties: {
                                            displayName: 'Nonie Radband',
                                            title: 'Analyst',
                                            firstName: 'Nonie',
                                            lastName: 'Radband',
                                            fullName: 'Nonie Radband',
                                            email: 'nonie@longtail.com',
                                            __typename: 'CorpUserProperties',
                                        },
                                        info: {
                                            active: true,
                                            displayName: 'Nonie Radband',
                                            title: 'Analyst',
                                            firstName: 'Nonie',
                                            lastName: 'Radband',
                                            fullName: 'Nonie Radband',
                                            email: 'nonie@longtail.com',
                                            __typename: 'CorpUserInfo',
                                        },
                                        __typename: 'CorpUser',
                                        editableProperties: null,
                                    },
                                    __typename: 'AggregationMetadata',
                                },
                                {
                                    value: 'urn:li:corpuser:melina@longtail.com',
                                    count: 1,
                                    entity: {
                                        urn: 'urn:li:corpuser:melina@longtail.com',
                                        type: 'CORP_USER',
                                        username: 'melina@longtail.com',
                                        properties: {
                                            displayName: 'Melina Eliez',
                                            title: 'Analyst',
                                            firstName: 'Melina',
                                            lastName: 'Eliez',
                                            fullName: 'Melina Eliez',
                                            email: 'melina@longtail.com',
                                            __typename: 'CorpUserProperties',
                                        },
                                        info: {
                                            active: true,
                                            displayName: 'Melina Eliez',
                                            title: 'Analyst',
                                            firstName: 'Melina',
                                            lastName: 'Eliez',
                                            fullName: 'Melina Eliez',
                                            email: 'melina@longtail.com',
                                            __typename: 'CorpUserInfo',
                                        },
                                        __typename: 'CorpUser',
                                        editableProperties: null,
                                    },
                                    __typename: 'AggregationMetadata',
                                },
                            ],
                            __typename: 'FacetMetadata',
                        },
                        {
                            field: '_entityType',
                            displayName: 'Type',
                            entity: null,
                            aggregations: [
                                {
                                    value: 'GLOSSARY_NODE',
                                    count: 17,
                                    entity: null,
                                    __typename: 'AggregationMetadata',
                                },
                                {
                                    value: 'GLOSSARY_TERM',
                                    count: 4,
                                    entity: null,
                                    __typename: 'AggregationMetadata',
                                },
                            ],
                            __typename: 'FacetMetadata',
                        },
                        {
                            field: 'hasParentNode',
                            displayName: 'hasParentNode',
                            entity: null,
                            aggregations: [
                                {
                                    value: 'false',
                                    count: 0,
                                    entity: null,
                                    __typename: 'AggregationMetadata',
                                },
                            ],
                            __typename: 'FacetMetadata',
                        },
                        {
                            field: 'entity',
                            displayName: 'Type',
                            entity: null,
                            aggregations: [
                                {
                                    value: 'GLOSSARY_NODE',
                                    count: 17,
                                    entity: null,
                                    __typename: 'AggregationMetadata',
                                },
                                {
                                    value: 'GLOSSARY_TERM',
                                    count: 4,
                                    entity: null,
                                    __typename: 'AggregationMetadata',
                                },
                            ],
                            __typename: 'FacetMetadata',
                        },
                    ],
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
        const selectedGlossaryItemPills = getAllByTestId('selected-item-pill');
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
        const selectedGlossaryItemPills = getAllByTestId('selected-item-pill');
        await waitFor(() => expect(selectedGlossaryItemPills.length).toBe(initialOptions.length));

        const dropdownClearIcon = getByTestId('dropdown-option-clear-icon');
        fireEvent.click(dropdownClearIcon);
        const selectedItemPill = screen.queryByTestId('selected-item-pill');
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
        const selectedGlossaryItemPills = getAllByTestId('selected-item-pill');
        // await waitFor(() => expect(selectedGlossaryItemPills.length).toBe(initialOptions.length));

        // const dropdownClearIcon = getByTestId('dropdown-option-clear-icon');
        // fireEvent.click(dropdownClearIcon);
        // const selectedItemPill = screen.queryByTestId('selected-item-pill');
        // expect(selectedItemPill).not.toBeInTheDocument();
    });

    // const mockOnSubmit = vi.fn(); // Create a mock function
});
