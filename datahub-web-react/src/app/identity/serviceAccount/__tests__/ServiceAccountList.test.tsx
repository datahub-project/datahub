import { MockedProvider } from '@apollo/client/testing';
import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ServiceAccountList } from '@app/identity/serviceAccount/ServiceAccountList';
import theme from '@src/alchemy-components/theme';

import { ListServiceAccountsDocument } from '@graphql/auth.generated';
import { ListRolesDocument } from '@graphql/role.generated';
import { EntityType } from '@types';

// Mock the user context
const mockUserContext = {
    platformPrivileges: {
        manageServiceAccounts: true,
    },
};

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => mockUserContext,
}));

// Mock the CreateServiceAccountModal
vi.mock('@app/identity/serviceAccount/CreateServiceAccountModal', () => ({
    default: ({ visible }: any) => (visible ? <div data-testid="create-modal">Create Modal</div> : null),
}));

// Mock the Message component
vi.mock('@app/shared/Message', () => ({
    Message: ({ content }: any) => <div data-testid="message">{content}</div>,
}));

// Mock scrollToTop
vi.mock('@app/shared/searchUtils', () => ({
    scrollToTop: vi.fn(),
}));

// Mock SimpleSelectRole
vi.mock('@app/identity/user/SimpleSelectRole', () => ({
    default: () => <div data-testid="select-role">Select Role</div>,
}));

// Mock CreateTokenModal
vi.mock('@app/settingsV2/CreateTokenModal', () => ({
    default: ({ visible }: any) => (visible ? <div data-testid="create-token-modal">Create Token Modal</div> : null),
}));

// Mock antd message
vi.mock('antd', async (importOriginal) => {
    const actual = await importOriginal<typeof import('antd')>();
    return {
        ...actual,
        message: {
            success: vi.fn(),
            error: vi.fn(),
            destroy: vi.fn(),
        },
    };
});

// Mock SearchBar component to avoid theme issues
vi.mock('@src/alchemy-components', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@src/alchemy-components')>();
    return {
        ...actual,
        SearchBar: ({ value, onChange, placeholder }: any) => (
            <input
                data-testid="search-bar-input"
                value={value}
                onChange={(e) => onChange(e.target.value)}
                placeholder={placeholder}
            />
        ),
    };
});

const mockServiceAccounts = [
    {
        __typename: 'ServiceAccount',
        urn: 'urn:li:serviceAccount:ingestion-pipeline',
        type: EntityType.CorpUser,
        name: 'ingestion-pipeline',
        displayName: 'Ingestion Pipeline',
        description: 'Service account for data ingestion',
        createdBy: 'urn:li:corpuser:datahub',
        createdAt: Date.now(),
        updatedAt: null,
        roles: {
            __typename: 'EntityRelationshipsResult',
            start: 0,
            count: 1,
            total: 0,
            relationships: [],
        },
    },
    {
        __typename: 'ServiceAccount',
        urn: 'urn:li:serviceAccount:monitoring-service',
        type: EntityType.CorpUser,
        name: 'monitoring-service',
        displayName: 'Monitoring Service',
        description: 'Service account for monitoring',
        createdBy: 'urn:li:corpuser:datahub',
        createdAt: Date.now(),
        updatedAt: null,
        roles: {
            __typename: 'EntityRelationshipsResult',
            start: 0,
            count: 1,
            total: 0,
            relationships: [],
        },
    },
];

const mocks = [
    {
        request: {
            query: ListServiceAccountsDocument,
            variables: {
                input: {
                    start: 0,
                    count: 10,
                    query: undefined,
                },
            },
        },
        result: {
            data: {
                listServiceAccounts: {
                    __typename: 'ListServiceAccountsResult',
                    start: 0,
                    count: 10,
                    total: 2,
                    serviceAccounts: mockServiceAccounts,
                },
            },
        },
    },
    {
        request: {
            query: ListRolesDocument,
            variables: {
                input: {
                    start: 0,
                    count: 10,
                },
            },
        },
        result: {
            data: {
                listRoles: {
                    __typename: 'ListRolesResult',
                    start: 0,
                    count: 10,
                    total: 2,
                    roles: [
                        {
                            __typename: 'DataHubRole',
                            urn: 'urn:li:dataHubRole:Admin',
                            type: EntityType.DatahubRole,
                            name: 'Admin',
                            description: 'Admin role',
                        },
                        {
                            __typename: 'DataHubRole',
                            urn: 'urn:li:dataHubRole:Editor',
                            type: EntityType.DatahubRole,
                            name: 'Editor',
                            description: 'Editor role',
                        },
                    ],
                },
            },
        },
    },
];

describe('ServiceAccountList', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockUserContext.platformPrivileges.manageServiceAccounts = true;
    });

    const renderComponent = () => {
        return render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <ThemeProvider theme={theme as any}>
                    <MemoryRouter>
                        <ServiceAccountList />
                    </MemoryRouter>
                </ThemeProvider>
            </MockedProvider>,
        );
    };

    // Note: The Create Service Account button is now in the page header (ManageUsersAndGroupsHeader)
    // and is tested separately. The inline button was removed as dead code.

    it('should render the search bar', async () => {
        renderComponent();

        await waitFor(
            () => {
                expect(screen.getByTestId('search-bar-input')).toBeInTheDocument();
            },
            { timeout: 2000 },
        );
    });

    it('should show access denied message when user lacks manageServiceAccounts privilege', () => {
        mockUserContext.platformPrivileges.manageServiceAccounts = false;

        renderComponent();

        expect(screen.getByText('Access Denied')).toBeInTheDocument();
        expect(screen.getByText('You do not have permission to manage service accounts.')).toBeInTheDocument();
    });

    it('should display service accounts when data is loaded', async () => {
        renderComponent();

        await waitFor(
            () => {
                expect(screen.getByText('Ingestion Pipeline')).toBeInTheDocument();
                expect(screen.getByText('Monitoring Service')).toBeInTheDocument();
            },
            { timeout: 3000 },
        );
    });
});
