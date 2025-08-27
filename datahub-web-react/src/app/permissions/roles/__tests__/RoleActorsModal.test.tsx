import { render, screen, fireEvent } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import RoleActorsModal from '@app/permissions/roles/RoleActorsModal';
import { CorpGroup, CorpUser, DataHubRole, EntityType } from '@types';

// Mock the GraphQL hooks
vi.mock('@graphql/mutations.generated', () => ({
    useBatchAssignRoleMutation: () => [
        vi.fn().mockResolvedValue({ data: { batchAssignRole: true } }),
        { loading: false },
    ],
}));

// Mock styled-components
vi.mock('styled-components', () => {
    const styledFactory = (Component: any) => () => {
        return ({ children, ...props }: any) => <Component {...props}>{children}</Component>;
    };
    styledFactory.div = styledFactory('div');
    styledFactory.Modal = styledFactory('div');
    return {
        __esModule: true,
        default: styledFactory,
    };
});

const mockUser: CorpUser = {
    urn: 'urn:li:corpuser:testuser',
    username: 'testuser',
    type: 'CORP_USER',
    info: {
        displayName: 'Test User',
        email: 'test@example.com',
    },
    editableProperties: {
        pictureLink: 'https://example.com/avatar.jpg',
    },
};

const mockGroup: CorpGroup = {
    urn: 'urn:li:corpGroup:testgroup',
    name: 'testgroup',
    type: 'CORP_GROUP',
    info: {
        displayName: 'Test Group',
        description: 'Test group description',
    },
};

const mockRole: DataHubRole = {
    urn: 'urn:li:dataHubRole:testRole',
    name: 'Test Role',
    type: EntityType.Dataset,

    __typename: 'DataHubRole',
    // Mock the relationships structure
    users: {
        relationships: [
            {
                entity: mockUser,
            },
        ],
    },
    groups: {
        relationships: [
            {
                entity: mockGroup,
            },
        ],
    },
} as any;

describe('RoleActorsModal', () => {
    const defaultProps = {
        visible: true,
        role: mockRole,
        onClose: vi.fn(),
        onSuccess: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('renders modal when visible', () => {
        render(<RoleActorsModal {...defaultProps} />);

        expect(screen.getByText('Manage Actors for "Test Role"')).toBeInTheDocument();
    });

    it('does not render when not visible', () => {
        render(<RoleActorsModal {...defaultProps} visible={false} />);

        expect(screen.queryByText('Manage Actors for "Test Role"')).not.toBeInTheDocument();
    });

    it('handles null role gracefully', () => {
        render(<RoleActorsModal {...defaultProps} role={null} />);

        expect(screen.queryByText(/manage actors/i)).not.toBeInTheDocument();
    });

    it('displays users assigned to role', () => {
        render(<RoleActorsModal {...defaultProps} />);

        expect(screen.getByText('Test User')).toBeInTheDocument();
        expect(screen.getByText('testuser')).toBeInTheDocument();
    });

    it('displays groups assigned to role', () => {
        render(<RoleActorsModal {...defaultProps} />);

        expect(screen.getByText('Test Group')).toBeInTheDocument();
    });

    it('shows correct actor count', () => {
        render(<RoleActorsModal {...defaultProps} />);

        expect(screen.getByText('2 actors assigned to this role')).toBeInTheDocument();
    });

    it('shows empty state when no actors', () => {
        const roleWithNoActors = {
            ...mockRole,
            users: { relationships: [] },
            groups: { relationships: [] },
        };

        render(<RoleActorsModal {...defaultProps} role={roleWithNoActors} />);

        expect(screen.getByText('No users or groups are assigned to this role')).toBeInTheDocument();
    });

    it('displays remove buttons for actors', () => {
        render(<RoleActorsModal {...defaultProps} />);

        const removeButtons = screen.getAllByTitle(/remove .* from role/i);
        expect(removeButtons).toHaveLength(2); // One for user, one for group
    });

    it('calls onClose when close button is clicked', () => {
        const onClose = vi.fn();

        render(<RoleActorsModal {...defaultProps} onClose={onClose} />);

        const closeButton = screen.getByRole('button', { name: /close/i });
        fireEvent.click(closeButton);

        expect(onClose).toHaveBeenCalled();
    });

    it('shows user icon and type indicator', () => {
        render(<RoleActorsModal {...defaultProps} />);

        expect(screen.getByText(/user/i)).toBeInTheDocument();
    });

    it('shows group icon and type indicator', () => {
        render(<RoleActorsModal {...defaultProps} />);

        expect(screen.getByText(/group/i)).toBeInTheDocument();
    });

    it('displays correct display names', () => {
        render(<RoleActorsModal {...defaultProps} />);

        expect(screen.getByText('Test User')).toBeInTheDocument();
        expect(screen.getByText('Test Group')).toBeInTheDocument();
    });

    it('falls back to username when display name unavailable', () => {
        const roleWithMinimalUserInfo = {
            ...mockRole,
            users: {
                relationships: [
                    {
                        entity: {
                            ...mockUser,
                            info: undefined,
                        },
                    },
                ],
            },
        };

        render(<RoleActorsModal {...defaultProps} role={roleWithMinimalUserInfo} />);

        expect(screen.getByText('testuser')).toBeInTheDocument();
    });

    it('falls back to group name when display name unavailable', () => {
        const roleWithMinimalGroupInfo = {
            ...mockRole,
            groups: {
                relationships: [
                    {
                        entity: {
                            ...mockGroup,
                            info: undefined,
                        },
                    },
                ],
            },
        };

        render(<RoleActorsModal {...defaultProps} role={roleWithMinimalGroupInfo} />);

        expect(screen.getByText('testgroup')).toBeInTheDocument();
    });

    it('handles actors with missing information gracefully', () => {
        const roleWithIncompleteActors = {
            ...mockRole,
            users: {
                relationships: [
                    {
                        entity: {
                            urn: 'urn:li:corpuser:incomplete',
                            username: 'incomplete',
                            type: 'CORP_USER',
                        },
                    },
                ],
            },
            groups: {
                relationships: [
                    {
                        entity: {
                            urn: 'urn:li:corpGroup:incomplete',
                            name: 'incomplete',
                            type: 'CORP_GROUP',
                        },
                    },
                ],
            },
        };

        render(<RoleActorsModal {...defaultProps} role={roleWithIncompleteActors} />);

        expect(screen.getByText('incomplete')).toBeInTheDocument();
    });
});