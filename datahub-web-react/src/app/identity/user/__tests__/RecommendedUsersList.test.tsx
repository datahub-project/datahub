import '@testing-library/jest-dom';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import RecommendedUsersList from '@app/identity/user/RecommendedUsersList';
import { CorpUser, DataHubRole, EntityType } from '@src/types.generated';

// Mock the platform constants
vi.mock('@app/ingest/source/builder/constants', () => ({
    PLATFORM_URN_TO_LOGO: {
        'urn:li:dataPlatform:bigquery': '/images/bigquery-logo.svg',
        'urn:li:dataPlatform:snowflake': '/images/snowflake-logo.svg',
    },
}));

// Mock the SimpleSelectRole component to avoid Apollo dependencies
vi.mock('../SimpleSelectRole', () => ({
    default: ({ selectedRole, onRoleSelect, size, width }: any) => (
        <div>
            <select
                data-testid="role-select"
                data-size={size}
                data-width={width}
                onChange={(e) =>
                    onRoleSelect(
                        e.target.value
                            ? { urn: e.target.value, name: e.target.options[e.target.selectedIndex].text }
                            : undefined,
                    )
                }
                value={selectedRole?.urn || ''}
            >
                <option value="">No Role</option>
                <option value="urn:li:role:viewer">Viewer</option>
                <option value="urn:li:role:editor">Editor</option>
                <option value="urn:li:role:admin">Admin</option>
            </select>
        </div>
    ),
}));

const mockRole: DataHubRole = {
    urn: 'urn:li:dataHubRole:viewer',
    type: EntityType.DatahubRole,
    name: 'Viewer',
    description: 'Can view all assets',
    __typename: 'DataHubRole',
};

const mockUser: CorpUser = {
    urn: 'urn:li:corpuser:testuser@company.com',
    type: EntityType.CorpUser,
    username: 'testuser@company.com',
    info: {
        email: 'testuser@company.com',
        firstName: 'Test',
        lastName: 'User',
        fullName: 'Test User',
        active: true,
    },
    properties: {
        email: 'testuser@company.com',
        active: true,
    },
    usageFeatures: {
        userUsagePercentilePast30Days: 95,
        userPlatformUsageTotalsPast30Days: [
            { key: 'urn:li:dataPlatform:bigquery', value: 100 },
            { key: 'urn:li:dataPlatform:snowflake', value: 50 },
        ],
    },
};

const mockUserLowUsage: CorpUser = {
    urn: 'urn:li:corpuser:lowuser@company.com',
    type: EntityType.CorpUser,
    username: 'lowuser@company.com',
    info: {
        email: 'lowuser@company.com',
        firstName: 'Low',
        lastName: 'User',
        fullName: 'Low User',
        active: true,
    },
    properties: {
        email: 'lowuser@company.com',
        active: true,
    },
    usageFeatures: {
        userUsagePercentilePast30Days: 50,
        userPlatformUsageTotalsPast30Days: [{ key: 'urn:li:dataPlatform:bigquery', value: 25 }],
    },
};

const renderWithTheme = (component: React.ReactElement) => {
    return render(component);
};

describe('RecommendedUsersList', () => {
    const mockOnInviteUser = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('renders empty state when no users provided', () => {
        renderWithTheme(
            <RecommendedUsersList recommendedUsers={[]} selectedRole={mockRole} onInviteUser={mockOnInviteUser} />,
        );

        expect(screen.getByText('No recommended users yet!')).toBeInTheDocument();
    });

    it('renders users with correct information', () => {
        renderWithTheme(
            <RecommendedUsersList
                recommendedUsers={[mockUser, mockUserLowUsage]}
                selectedRole={mockRole}
                onInviteUser={mockOnInviteUser}
            />,
        );

        expect(screen.getByText('testuser@company.com')).toBeInTheDocument();
        expect(screen.getByText('lowuser@company.com')).toBeInTheDocument();
    });

    it('shows "Top User" pill for users with high usage percentile', () => {
        renderWithTheme(
            <RecommendedUsersList
                recommendedUsers={[mockUser, mockUserLowUsage]}
                selectedRole={mockRole}
                onInviteUser={mockOnInviteUser}
            />,
        );

        expect(screen.getByText('Top User')).toBeInTheDocument();
        expect(screen.getAllByText('Top User')).toHaveLength(1); // Only high usage user should have it
    });

    it('calls onInviteUser when invite button is clicked', () => {
        renderWithTheme(
            <RecommendedUsersList
                recommendedUsers={[mockUser]}
                selectedRole={mockRole}
                onInviteUser={mockOnInviteUser}
            />,
        );

        const inviteButton = screen.getByText('Invite');
        fireEvent.click(inviteButton);

        expect(mockOnInviteUser).toHaveBeenCalledWith(mockUser, mockRole);
    });

    it('shows success state after successful invitation', () => {
        const userStates = {
            [mockUser.urn]: { status: 'success' as const, role: mockRole },
        };

        renderWithTheme(
            <RecommendedUsersList
                recommendedUsers={[mockUser]}
                selectedRole={mockRole}
                onInviteUser={mockOnInviteUser}
                userStates={userStates}
            />,
        );

        expect(screen.getByText('Invited as Viewer')).toBeInTheDocument();
        expect(screen.queryByText('Invite')).not.toBeInTheDocument();
    });

    it('shows failure state after failed invitation', () => {
        const userStates = {
            [mockUser.urn]: { status: 'failed' as const, role: mockRole },
        };

        renderWithTheme(
            <RecommendedUsersList
                recommendedUsers={[mockUser]}
                selectedRole={mockRole}
                onInviteUser={mockOnInviteUser}
                userStates={userStates}
            />,
        );

        expect(screen.getByText('Invitation failed')).toBeInTheDocument();
        expect(screen.queryByText('Invite')).not.toBeInTheDocument();
    });

    it('filters out hidden users', () => {
        const hiddenUsers = new Set([mockUser.urn]);

        renderWithTheme(
            <RecommendedUsersList
                recommendedUsers={[mockUser, mockUserLowUsage]}
                selectedRole={mockRole}
                onInviteUser={mockOnInviteUser}
                hiddenUsers={hiddenUsers}
            />,
        );

        expect(screen.queryByText('testuser@company.com')).not.toBeInTheDocument();
        expect(screen.getByText('lowuser@company.com')).toBeInTheDocument();
    });

    it('limits displayed users to RECOMMENDED_USERS_DISPLAY_COUNT', () => {
        // Create 10 users (more than the display limit of 6)
        const manyUsers = Array.from({ length: 10 }, (_, i) => ({
            ...mockUser,
            urn: `urn:li:corpuser:user${i}@company.com`,
            username: `user${i}@company.com`,
        }));

        renderWithTheme(
            <RecommendedUsersList
                recommendedUsers={manyUsers}
                selectedRole={mockRole}
                onInviteUser={mockOnInviteUser}
            />,
        );

        // Should display 6 users (the first 6 from the array)
        expect(screen.getByText('user0@company.com')).toBeInTheDocument();
        expect(screen.getByText('user5@company.com')).toBeInTheDocument();
        // Should not display more than 6 users
        expect(screen.queryByText('user6@company.com')).not.toBeInTheDocument();
    });
});
