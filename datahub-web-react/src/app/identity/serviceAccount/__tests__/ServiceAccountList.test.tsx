import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ServiceAccountList } from '@app/identity/serviceAccount/ServiceAccountList';
import defaultThemeConfig from '@conf/theme/theme_light.config.json';

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

// Mock the GraphQL hooks
const mockListServiceAccounts = {
    loading: false,
    error: null,
    data: {
        listServiceAccounts: {
            start: 0,
            count: 10,
            total: 2,
            serviceAccounts: [
                {
                    urn: 'urn:li:serviceAccount:ingestion-pipeline',
                    type: EntityType.CorpUser,
                    name: 'ingestion-pipeline',
                    displayName: 'Ingestion Pipeline',
                    description: 'Service account for data ingestion',
                    createdBy: 'urn:li:corpuser:datahub',
                    createdAt: Date.now(),
                    updatedAt: null,
                },
                {
                    urn: 'urn:li:serviceAccount:monitoring-service',
                    type: EntityType.CorpUser,
                    name: 'monitoring-service',
                    displayName: 'Monitoring Service',
                    description: 'Service account for monitoring',
                    createdBy: 'urn:li:corpuser:datahub',
                    createdAt: Date.now(),
                    updatedAt: null,
                },
            ],
        },
    },
    refetch: vi.fn(),
};

vi.mock('@graphql/auth.generated', () => ({
    useListServiceAccountsQuery: () => mockListServiceAccounts,
    useDeleteServiceAccountMutation: () => [vi.fn(), { loading: false }],
}));

// Mock the CreateServiceAccountModal
vi.mock('@app/identity/serviceAccount/CreateServiceAccountModal', () => ({
    default: ({ visible }: any) => (visible ? <div data-testid="create-modal">Create Modal</div> : null),
}));

// Mock the ServiceAccountListItem
vi.mock('@app/identity/serviceAccount/ServiceAccountListItem', () => ({
    default: ({ serviceAccount }: any) => (
        <div data-testid={`service-account-item-${serviceAccount.name}`}>{serviceAccount.displayName}</div>
    ),
}));

// Mock the entity registry
vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getDisplayName: () => 'Test',
    }),
}));

// Mock the Message component
vi.mock('@app/shared/Message', () => ({
    Message: ({ content }: any) => <div data-testid="message">{content}</div>,
}));

// Mock the SearchBar
vi.mock('@app/search/SearchBar', () => ({
    SearchBar: () => <input data-testid="search-bar" />,
}));

// Mock TabToolbar
vi.mock('@app/entity/shared/components/styled/TabToolbar', () => ({
    default: ({ children }: any) => <div data-testid="tab-toolbar">{children}</div>,
}));

// Mock scrollToTop
vi.mock('@app/shared/searchUtils', () => ({
    scrollToTop: vi.fn(),
}));

describe('ServiceAccountList', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockUserContext.platformPrivileges.manageServiceAccounts = true;
    });

    const renderWithRouter = (component: React.ReactNode) => {
        return render(
            <ThemeProvider theme={defaultThemeConfig}>
                <MemoryRouter>{component}</MemoryRouter>
            </ThemeProvider>,
        );
    };

    it('should render the service account list', async () => {
        renderWithRouter(<ServiceAccountList />);

        await waitFor(() => {
            expect(screen.getByTestId('service-account-item-ingestion-pipeline')).toBeInTheDocument();
            expect(screen.getByTestId('service-account-item-monitoring-service')).toBeInTheDocument();
        });
    });

    it('should render the Create Service Account button', () => {
        renderWithRouter(<ServiceAccountList />);

        expect(screen.getByTestId('create-service-account-button')).toBeInTheDocument();
    });

    it('should render the search bar', () => {
        renderWithRouter(<ServiceAccountList />);

        expect(screen.getByTestId('search-bar')).toBeInTheDocument();
    });

    it('should show permission denied message when user lacks manageServiceAccounts privilege', () => {
        mockUserContext.platformPrivileges.manageServiceAccounts = false;

        renderWithRouter(<ServiceAccountList />);

        expect(screen.getByText('You do not have permission to manage service accounts.')).toBeInTheDocument();
    });

    it('should display the correct number of service accounts', async () => {
        renderWithRouter(<ServiceAccountList />);

        await waitFor(() => {
            expect(screen.getByText('Ingestion Pipeline')).toBeInTheDocument();
            expect(screen.getByText('Monitoring Service')).toBeInTheDocument();
        });
    });
});
