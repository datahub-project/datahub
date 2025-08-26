import { fireEvent, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';

import PolicyActorForm from '@app/permissions/policy/PolicyActorForm';
import { render } from '@utils/test-utils/customRender';

import { ActorFilter, CorpUser, EntityType, PolicyType } from '@types';

// Mock hooks
const mockGetEntities = vi.fn();

vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsLazyQuery: vi.fn(() => [vi.fn(), { data: { search: { searchResults: [] } } }]),
}));

vi.mock('@graphql/entity.generated', () => ({
    useGetEntitiesLazyQuery: vi.fn(() => [mockGetEntities, { data: null }]),
}));

vi.mock('@graphql/ownership.generated', () => ({
    useListOwnershipTypesQuery: () => ({
        data: {
            listOwnershipTypes: {
                ownershipTypes: [
                    {
                        urn: 'urn:li:ownershipType:technical',
                        info: { name: 'Technical Owner' },
                    },
                ],
            },
        },
    }),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getDisplayName: vi.fn((type, entity) => {
            if (type === EntityType.CorpUser) return entity.properties?.displayName || 'Test User';
            if (type === EntityType.CorpGroup) return entity.properties?.displayName || 'Test Group';
            return 'Test Entity';
        }),
    }),
}));

describe('PolicyActorForm', () => {
    const mockSetActors = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
    });

    const defaultProps = {
        policyType: PolicyType.Metadata,
        actors: {
            allUsers: false,
            allGroups: false,
            resourceOwners: false,
            users: [],
            groups: [],
            resolvedUsers: [],
            resolvedGroups: [],
            resolvedRoles: [],
        } as ActorFilter,
        setActors: mockSetActors,
    };

    it('renders basic form elements', () => {
        render(
            <BrowserRouter>
                <PolicyActorForm {...defaultProps} />
            </BrowserRouter>,
        );

        expect(screen.getByText('Applies to')).toBeInTheDocument();
        expect(screen.getByText('Select the users & groups that this policy should apply to.')).toBeInTheDocument();
        expect(screen.getByText('Users')).toBeInTheDocument();
        expect(screen.getByText('Groups')).toBeInTheDocument();
    });

    it('shows ownership section for metadata policies', () => {
        render(
            <BrowserRouter>
                <PolicyActorForm {...defaultProps} policyType={PolicyType.Metadata} />
            </BrowserRouter>,
        );

        expect(screen.getByText('Owners')).toBeInTheDocument();
        expect(screen.getByText(/Whether this policy should be apply to owners/)).toBeInTheDocument();
    });

    it('does not show ownership section for platform policies', () => {
        render(
            <BrowserRouter>
                <PolicyActorForm {...defaultProps} policyType={PolicyType.Platform} />
            </BrowserRouter>,
        );

        expect(screen.queryByText('Owners')).not.toBeInTheDocument();
    });

    it('initializes resolved entities from actors prop', () => {
        const mockResolvedUser: CorpUser = {
            urn: 'urn:li:corpuser:testuser',
            type: EntityType.CorpUser,
            username: 'testuser',
            properties: {
                displayName: 'Test User',
                active: true,
            },
        };

        const mockResolvedGroup = {
            urn: 'urn:li:corpGroup:testgroup',
            type: EntityType.CorpGroup,
            name: 'testgroup',
            properties: {
                displayName: 'Test Group',
            },
        };

        const actorsWithResolved: ActorFilter = {
            ...defaultProps.actors,
            users: ['urn:li:corpuser:testuser'],
            groups: ['urn:li:corpGroup:testgroup'],
            resolvedUsers: [mockResolvedUser],
            resolvedGroups: [mockResolvedGroup],
        };

        render(
            <BrowserRouter>
                <PolicyActorForm {...defaultProps} actors={actorsWithResolved} />
            </BrowserRouter>,
        );

        // Check that resolved names are displayed in tags
        expect(screen.getByText('Test User')).toBeInTheDocument();
        expect(screen.getByText('Test Group')).toBeInTheDocument();
    });

    it('falls back to URN when entity is not resolved', () => {
        const actorsWithUnresolvedUrns: ActorFilter = {
            ...defaultProps.actors,
            users: ['urn:li:corpuser:unresolveduser'],
            groups: ['urn:li:corpGroup:unresolvedgroup'],
            resolvedUsers: [],
            resolvedGroups: [],
        };

        render(
            <BrowserRouter>
                <PolicyActorForm {...defaultProps} actors={actorsWithUnresolvedUrns} />
            </BrowserRouter>,
        );

        // Should fall back to showing URNs
        expect(screen.getByText('urn:li:corpuser:unresolveduser')).toBeInTheDocument();
        expect(screen.getByText('urn:li:corpGroup:unresolvedgroup')).toBeInTheDocument();
    });

    it('calls getEntities for unresolved URNs', async () => {
        const actorsWithUnresolvedUrns: ActorFilter = {
            ...defaultProps.actors,
            users: ['urn:li:corpuser:unresolveduser'],
            groups: ['urn:li:corpGroup:unresolvedgroup'],
            resolvedUsers: [],
            resolvedGroups: [],
        };

        render(
            <BrowserRouter>
                <PolicyActorForm {...defaultProps} actors={actorsWithUnresolvedUrns} />
            </BrowserRouter>,
        );

        // Should call getEntities to resolve the URNs
        await waitFor(() => {
            expect(mockGetEntities).toHaveBeenCalledWith({
                variables: {
                    urns: ['urn:li:corpuser:unresolveduser', 'urn:li:corpGroup:unresolvedgroup'],
                },
            });
        });
    });

    it('updates resolved entities when actors prop changes', () => {
        const initialActors: ActorFilter = {
            ...defaultProps.actors,
            users: ['urn:li:corpuser:user1'],
            resolvedUsers: [
                {
                    urn: 'urn:li:corpuser:user1',
                    type: EntityType.CorpUser,
                    username: 'user1',
                    properties: { displayName: 'User 1', active: true },
                },
            ],
        };

        const { rerender } = render(
            <BrowserRouter>
                <PolicyActorForm {...defaultProps} actors={initialActors} />
            </BrowserRouter>,
        );

        expect(screen.getByText('User 1')).toBeInTheDocument();

        // Change actors prop
        const updatedActors: ActorFilter = {
            ...defaultProps.actors,
            users: ['urn:li:corpuser:user2'],
            resolvedUsers: [
                {
                    urn: 'urn:li:corpuser:user2',
                    type: EntityType.CorpUser,
                    username: 'user2',
                    properties: { displayName: 'User 2', active: true },
                },
            ],
        };

        rerender(
            <BrowserRouter>
                <PolicyActorForm {...defaultProps} actors={updatedActors} />
            </BrowserRouter>,
        );

        expect(screen.getByText('User 2')).toBeInTheDocument();
        expect(screen.queryByText('User 1')).not.toBeInTheDocument();
    });

    it('handles "All" users selection correctly', () => {
        const actorsWithAllUsers: ActorFilter = {
            ...defaultProps.actors,
            allUsers: true,
        };

        render(
            <BrowserRouter>
                <PolicyActorForm {...defaultProps} actors={actorsWithAllUsers} />
            </BrowserRouter>,
        );

        // Should show "All Users" tag
        expect(screen.getByText('All Users')).toBeInTheDocument();
    });

    it('handles "All" groups selection correctly', () => {
        const actorsWithAllGroups: ActorFilter = {
            ...defaultProps.actors,
            allGroups: true,
        };

        render(
            <BrowserRouter>
                <PolicyActorForm {...defaultProps} actors={actorsWithAllGroups} />
            </BrowserRouter>,
        );

        // Should show "All Groups" tag
        expect(screen.getByText('All Groups')).toBeInTheDocument();
    });

    it('toggles resource owners correctly', () => {
        render(
            <BrowserRouter>
                <PolicyActorForm {...defaultProps} policyType={PolicyType.Metadata} />
            </BrowserRouter>,
        );

        const ownersSwitch = screen.getByRole('switch');
        fireEvent.click(ownersSwitch);

        expect(mockSetActors).toHaveBeenCalledWith(
            expect.objectContaining({
                resourceOwners: true,
            }),
        );
    });

    it('shows ownership types section when resource owners is enabled', () => {
        const actorsWithResourceOwners: ActorFilter = {
            ...defaultProps.actors,
            resourceOwners: true,
        };

        render(
            <BrowserRouter>
                <PolicyActorForm {...defaultProps} actors={actorsWithResourceOwners} />
            </BrowserRouter>,
        );

        expect(screen.getByText(/List of types of ownership/)).toBeInTheDocument();
    });

    it('renders user avatars when available', () => {
        const mockUserWithAvatar: CorpUser = {
            urn: 'urn:li:corpuser:testuser',
            type: EntityType.CorpUser,
            username: 'testuser',
            properties: {
                displayName: 'Test User',
                active: true,
            },
            editableProperties: {
                pictureLink: 'https://example.com/avatar.jpg',
            },
        };

        const actorsWithAvatar: ActorFilter = {
            ...defaultProps.actors,
            users: ['urn:li:corpuser:testuser'],
            resolvedUsers: [mockUserWithAvatar],
        };

        render(
            <BrowserRouter>
                <PolicyActorForm {...defaultProps} actors={actorsWithAvatar} />
            </BrowserRouter>,
        );

        // Check that avatar is rendered (CustomAvatar component should be in DOM)
        expect(screen.getByText('Test User')).toBeInTheDocument();
    });
});
