import { MockedProvider } from '@apollo/client/testing';
import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';

import { ProposalList } from '@app/taskCenterV2/proposalsV2/ProposalList';
import { ListActionRequestsDocument } from '@src/graphql/actionRequest.generated';

const mocks = [
    {
        request: {
            query: ListActionRequestsDocument,
            variables: {
                input: {
                    start: 0,
                    count: 25,
                    orFilters: [],
                },
            },
        },
        result: {
            data: {
                listActionRequests: {
                    actionRequests: [],
                    total: 0,
                    facets: [
                        { field: 'createdBy', aggregations: [] },
                        { field: 'status', aggregations: [] },
                        { field: 'type', aggregations: [] },
                    ],
                },
            },
        },
    },
];

// TODO: Add non-filter tests
describe('Renders the Filters properly', () => {
    it.skip('renders the FilterSection component', async () => {
        render(
            <MemoryRouter>
                <MockedProvider mocks={mocks} addTypename={false}>
                    <ProposalList />
                </MockedProvider>
            </MemoryRouter>,
        );

        await waitFor(() => {
            expect(screen.getByTestId('proposals-filters-section')).toBeInTheDocument();
        });
    });

    it.skip('renders the default filters', async () => {
        render(
            <MemoryRouter>
                <MockedProvider mocks={mocks} addTypename={false}>
                    <ProposalList />
                </MockedProvider>
            </MemoryRouter>,
        );

        await waitFor(() => {
            // Check all three filters are in the document
            expect(screen.getByTestId('filter-dropdown-Created-By')).toBeInTheDocument();
            expect(screen.getByTestId('filter-dropdown-Type')).toBeInTheDocument();
            expect(screen.getByTestId('filter-dropdown-Status')).toBeInTheDocument();
        });
    });
});
