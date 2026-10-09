import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ManageDataAccessRoles } from '@app/permissions/dataAccessRoles/ManageDataAccessRoles';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const mockUseListDataAccessRolesQuery = vi.hoisted(() => vi.fn());

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

describe('ManageDataAccessRoles', () => {
    beforeEach(() => {
        mockUseListDataAccessRolesQuery.mockReset();
    });

    it('renders the role, access type, and provisioned actors', () => {
        mockUseListDataAccessRolesQuery.mockReturnValue({ loading: false, error: undefined, data: roleData() });

        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <ManageDataAccessRoles />
                </TestPageContainer>
            </MockedProvider>,
        );

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

        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <ManageDataAccessRoles />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('No data access roles!')).toBeInTheDocument();
    });
});
