import { render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ManageIdentities } from '@app/identity/ManageIdentities';

// Mock the user context
const mockUserContext = {
    platformPrivileges: {
        manageServiceAccounts: false,
    },
};

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => mockUserContext,
}));

// Mock the child components
vi.mock('@app/identity/group/GroupList', () => ({
    GroupList: () => <div data-testid="group-list">Group List</div>,
}));

vi.mock('@app/identity/user/UserList', () => ({
    UserList: () => <div data-testid="user-list">User List</div>,
}));

vi.mock('@app/identity/serviceAccount', () => ({
    ServiceAccountList: () => <div data-testid="service-account-list">Service Account List</div>,
}));

// Mock RoutedTabs
vi.mock('@app/shared/RoutedTabs', () => ({
    RoutedTabs: ({ tabs }: { tabs: Array<{ name: string; path: string; content: React.ReactNode }> }) => (
        <div data-testid="routed-tabs">
            {tabs.map((tab) => (
                <div key={tab.path} data-testid={`tab-${tab.path}`}>
                    <span data-testid={`tab-name-${tab.path}`}>{tab.name}</span>
                    {tab.content}
                </div>
            ))}
        </div>
    ),
}));

describe('ManageIdentities', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockUserContext.platformPrivileges.manageServiceAccounts = false;
    });

    const renderWithRouter = (component: React.ReactNode) => {
        return render(<MemoryRouter>{component}</MemoryRouter>);
    };

    it('should render the page title', () => {
        renderWithRouter(<ManageIdentities />);

        expect(screen.getByText('Manage Users & Groups')).toBeInTheDocument();
    });

    it('should render Users and Groups tabs by default', () => {
        renderWithRouter(<ManageIdentities />);

        expect(screen.getByTestId('tab-users')).toBeInTheDocument();
        expect(screen.getByTestId('tab-groups')).toBeInTheDocument();
    });

    it('should not render Service Accounts tab when user lacks permission', () => {
        mockUserContext.platformPrivileges.manageServiceAccounts = false;

        renderWithRouter(<ManageIdentities />);

        expect(screen.queryByTestId('tab-service-accounts')).not.toBeInTheDocument();
    });

    it('should render Service Accounts tab when user has permission', () => {
        mockUserContext.platformPrivileges.manageServiceAccounts = true;

        renderWithRouter(<ManageIdentities />);

        expect(screen.getByTestId('tab-service-accounts')).toBeInTheDocument();
        expect(screen.getByTestId('tab-name-service-accounts')).toHaveTextContent('Service Accounts');
    });

    it('should render the UserList component in the Users tab', () => {
        renderWithRouter(<ManageIdentities />);

        expect(screen.getByTestId('user-list')).toBeInTheDocument();
    });

    it('should render the GroupList component in the Groups tab', () => {
        renderWithRouter(<ManageIdentities />);

        expect(screen.getByTestId('group-list')).toBeInTheDocument();
    });

    it('should render ServiceAccountList when permission is granted', () => {
        mockUserContext.platformPrivileges.manageServiceAccounts = true;

        renderWithRouter(<ManageIdentities />);

        expect(screen.getByTestId('service-account-list')).toBeInTheDocument();
    });
});
