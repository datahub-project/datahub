import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ManageDataAccessRoles } from '@app/permissions/dataAccessRoles/ManageDataAccessRoles';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const mockUseListDataAccessRolesQuery = vi.hoisted(() => vi.fn());
const mockShowToastMessage = vi.hoisted(() => vi.fn());

vi.mock('@app/sharedV2/toastMessageUtils', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@app/sharedV2/toastMessageUtils')>();
    return { ...actual, showToastMessage: mockShowToastMessage };
});

vi.mock('@graphql/dataAccessRole.generated', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@graphql/dataAccessRole.generated')>();
    return { ...actual, useListDataAccessRolesQuery: (args: any) => mockUseListDataAccessRolesQuery(args) };
});

function roleData() {
    return {
        search: {
            start: 0,
            count: 1,
            total: 1,
            searchResults: [
                {
                    entity: {
                        __typename: 'Role',
                        urn: 'urn:li:role:analyst',
                        type: 'ROLE',
                        properties: {
                            name: 'Analyst',
                            description: 'Read warehouse tables',
                            type: 'READ',
                            requestUrl: 'https://access.example/analyst',
                        },
                        actors: {
                            users: [
                                {
                                    user: {
                                        urn: 'urn:li:corpuser:alice',
                                        type: 'CORP_USER',
                                        username: 'alice',
                                        properties: { displayName: 'Alice' },
                                    },
                                },
                                { user: null },
                            ],
                            groups: [
                                { group: { urn: 'urn:li:corpGroup:finance', type: 'CORP_GROUP', name: 'Finance' } },
                            ],
                        },
                    },
                },
            ],
        },
    };
}

function renderPage() {
    return render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <ManageDataAccessRoles />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('ManageDataAccessRoles', () => {
    beforeEach(() => {
        mockUseListDataAccessRolesQuery.mockReset();
        mockShowToastMessage.mockReset();
    });

    it('renders the role, access type, and provisioned actors', () => {
        mockUseListDataAccessRolesQuery.mockReturnValue({ loading: false, error: undefined, data: roleData() });

        renderPage();

        expect(screen.getByText('Analyst')).toBeInTheDocument();
        expect(screen.getByText('Read warehouse tables')).toBeInTheDocument();
        expect(screen.getByText('READ')).toBeInTheDocument();
    });

    it('renders the empty state when there are no roles', () => {
        mockUseListDataAccessRolesQuery.mockReturnValue({
            loading: false,
            error: undefined,
            data: { search: { start: 0, count: 0, total: 0, searchResults: [] } },
        });

        renderPage();

        expect(screen.getByText('No data access roles!')).toBeInTheDocument();
    });

    it('opens the role details when a row is clicked', () => {
        mockUseListDataAccessRolesQuery.mockReturnValue({ loading: false, error: undefined, data: roleData() });

        renderPage();
        fireEvent.click(screen.getByText('Analyst'));

        const modal = within(screen.getByRole('dialog'));
        expect(modal.getByText('Read warehouse tables')).toBeInTheDocument();
        expect(modal.getByText('READ')).toBeInTheDocument();
        expect(modal.getByText('Alice')).toBeInTheDocument();
        expect(modal.getByText('Finance')).toBeInTheDocument();
        expect(modal.getByText('https://access.example/analyst')).toBeInTheDocument();
    });

    it('queries with the typed search text', async () => {
        mockUseListDataAccessRolesQuery.mockReturnValue({ loading: false, error: undefined, data: roleData() });

        renderPage();
        fireEvent.change(screen.getByPlaceholderText('Search data access roles...'), { target: { value: 'analyst' } });

        await waitFor(() =>
            expect(mockUseListDataAccessRolesQuery).toHaveBeenLastCalledWith(
                expect.objectContaining({ variables: { input: expect.objectContaining({ query: 'analyst' }) } }),
            ),
        );
    });

    it('shows an error toast and no empty state when the query fails', () => {
        mockUseListDataAccessRolesQuery.mockReturnValue({ loading: false, error: new Error('boom'), data: undefined });

        renderPage();

        expect(mockShowToastMessage).toHaveBeenCalled();
        expect(screen.queryByText('No data access roles!')).not.toBeInTheDocument();
    });

    it('requests the next page when pagination changes', async () => {
        const data = roleData();
        data.search.total = 25;
        mockUseListDataAccessRolesQuery.mockReturnValue({ loading: false, error: undefined, data });

        renderPage();
        fireEvent.click(screen.getByText('2'));

        await waitFor(() =>
            expect(mockUseListDataAccessRolesQuery).toHaveBeenLastCalledWith(
                expect.objectContaining({ variables: { input: expect.objectContaining({ start: 10 }) } }),
            ),
        );
    });
});
