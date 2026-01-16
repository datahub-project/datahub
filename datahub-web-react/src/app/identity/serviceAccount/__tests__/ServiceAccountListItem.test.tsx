import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import ServiceAccountListItem from '@app/identity/serviceAccount/ServiceAccountListItem';

import { EntityType, ServiceAccount } from '@types';

// Mock the CreateServiceAccountTokenModal
vi.mock('@app/identity/serviceAccount/CreateServiceAccountTokenModal', () => ({
    default: ({ visible }: any) => (visible ? <div data-testid="create-token-modal">Create Token Modal</div> : null),
}));

// Mock CustomAvatar
vi.mock('@app/shared/avatar/CustomAvatar', () => ({
    default: ({ name }: any) => <div data-testid="custom-avatar">{name}</div>,
}));

// Mock time utils
vi.mock('@app/shared/time/timeUtils', () => ({
    getLocaleTimezone: () => 'UTC',
}));

describe('ServiceAccountListItem', () => {
    const mockServiceAccount: ServiceAccount = {
        urn: 'urn:li:serviceAccount:test-service-account',
        type: EntityType.CorpUser, // Using CorpUser as placeholder since ServiceAccount may not be in EntityType yet
        name: 'test-service-account',
        displayName: 'Test Service Account',
        description: 'A test service account for API access',
        createdBy: 'urn:li:corpuser:datahub',
        createdAt: Date.now(),
        updatedAt: null,
    };

    const defaultProps = {
        serviceAccount: mockServiceAccount,
        onDelete: vi.fn(),
        onCreateToken: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    const renderWithRouter = (component: React.ReactNode) => {
        return render(<MemoryRouter>{component}</MemoryRouter>);
    };

    it('should render the service account display name', () => {
        renderWithRouter(<ServiceAccountListItem {...defaultProps} />);

        // Display name appears in both header and avatar
        expect(screen.getAllByText('Test Service Account').length).toBeGreaterThanOrEqual(1);
    });

    it('should render the service account name', () => {
        renderWithRouter(<ServiceAccountListItem {...defaultProps} />);

        expect(screen.getByText('test-service-account')).toBeInTheDocument();
    });

    it('should render the service account description', () => {
        renderWithRouter(<ServiceAccountListItem {...defaultProps} />);

        expect(screen.getByText('A test service account for API access')).toBeInTheDocument();
    });

    it('should show the action menu when dropdown is clicked', async () => {
        renderWithRouter(<ServiceAccountListItem {...defaultProps} />);

        // Find and click the menu icon
        const menuIcon = screen.getByTestId('service-account-menu-test-service-account');
        fireEvent.click(menuIcon);

        // Check that menu items appear
        await waitFor(() => {
            expect(screen.getByTestId('create-token-menu-item')).toBeInTheDocument();
            expect(screen.getByTestId('delete-service-account-menu-item')).toBeInTheDocument();
        });
    });

    it('should fallback to name if displayName is not provided', () => {
        const propsWithoutDisplayName = {
            ...defaultProps,
            serviceAccount: {
                ...mockServiceAccount,
                displayName: null,
            },
        };

        renderWithRouter(<ServiceAccountListItem {...propsWithoutDisplayName} />);

        // Should show the name - appears in header, subtitle, and avatar
        expect(screen.getAllByText('test-service-account').length).toBeGreaterThanOrEqual(2);
    });

    it('should render CustomAvatar with the display name', () => {
        renderWithRouter(<ServiceAccountListItem {...defaultProps} />);

        expect(screen.getByTestId('custom-avatar')).toHaveTextContent('Test Service Account');
    });

    it('should show created at date when provided', () => {
        renderWithRouter(<ServiceAccountListItem {...defaultProps} />);

        expect(screen.getByText(/Created:/)).toBeInTheDocument();
    });
});
