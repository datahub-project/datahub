import { render, screen } from '@testing-library/react';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';

import PolicyDetailsModal from '@app/permissions/policy/PolicyDetailsModal';

import { EntityType, Policy, PolicyMatchCondition, PolicyState, PolicyType } from '@types';

// Mock the hooks
vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getEntityUrl: vi.fn().mockReturnValue('/test'),
        getDisplayName: vi.fn().mockReturnValue('Test Entity'),
    }),
}));

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => ({
        config: {
            policiesConfig: {
                resourcePrivileges: [
                    {
                        resourceType: 'dataset',
                        resourceTypeDisplayName: 'Dataset',
                        privileges: [
                            {
                                type: 'view',
                                displayName: 'View',
                            },
                        ],
                    },
                ],
            },
        },
    }),
}));

// Mock AvatarsGroup component to avoid rendering issues
vi.mock('@app/permissions/AvatarsGroup', () => ({
    default: () => <div data-testid="avatars-group">Avatar Group Mock</div>,
}));

// Default mock policy
const mockPolicy: Omit<Policy, 'urn'> = {
    type: PolicyType.Metadata,
    name: 'Test Policy',
    editable: true,
    description: 'A test policy',
    state: PolicyState.Active,
    privileges: ['view'],
    resources: {
        filter: {
            criteria: [
                {
                    field: 'TYPE',
                    values: [{ value: 'dataset' }],
                    condition: PolicyMatchCondition.Equals,
                },
                {
                    field: 'URN',
                    values: [
                        {
                            value: 'urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)',
                            entity: {
                                type: EntityType.Dataset,
                                urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)',
                            },
                        },
                    ],
                    condition: PolicyMatchCondition.Equals,
                },
            ],
        },
    },
    actors: {
        allUsers: false,
        allGroups: false,
        resourceOwners: false,
        resolvedUsers: [],
        resolvedGroups: [],
        resolvedRoles: [],
    },
};

// Mock policy with containers
const mockPolicyWithContainers = {
    ...mockPolicy,
    resources: {
        filter: {
            criteria: [
                ...(mockPolicy.resources?.filter?.criteria || []),
                {
                    field: 'CONTAINER',
                    values: [
                        {
                            value: 'urn:li:container:testContainer',
                            entity: {
                                type: EntityType.Container,
                                urn: 'urn:li:container:testContainer',
                            },
                        },
                    ],
                    condition: PolicyMatchCondition.Equals,
                },
            ],
        },
    },
};

// Mock policy with resource owners
const mockPolicyWithResourceOwners = {
    ...mockPolicy,
    type: PolicyType.Metadata,
    actors: {
        ...mockPolicy.actors,
        resourceOwners: true,
        resolvedOwnershipTypes: [
            {
                urn: 'urn:li:ownershipType:TECHNICAL_OWNER',
                type: EntityType.CustomOwnershipType,
                info: { name: 'Technical Owner' },
            },
        ],
    },
};

describe('PolicyDetailsModal', () => {
    // Suppress console warnings about missing keys
    beforeEach(() => {
        vi.spyOn(console, 'error').mockImplementation(() => {});
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('renders policy details correctly', () => {
        render(
            <BrowserRouter>
                <PolicyDetailsModal
                    policy={mockPolicy}
                    open
                    onClose={() => {}}
                    privileges={[{ type: 'view', name: 'View' }]}
                />
            </BrowserRouter>,
        );

        // Check the modal has rendered correctly
        expect(screen.getByText('Test Policy')).toBeInTheDocument();
        expect(screen.getByText('A test policy')).toBeInTheDocument();
        expect(screen.getByText('View')).toBeInTheDocument();

        // Check that "Containers" section is rendered
        expect(screen.getByText('Containers')).toBeInTheDocument();
        // Verify "All" is displayed when no containers
        const allTags = screen.getAllByText('All');
        expect(allTags.length).toBeGreaterThanOrEqual(2);
    });

    it('renders containers when provided', () => {
        render(
            <BrowserRouter>
                <PolicyDetailsModal
                    policy={mockPolicyWithContainers}
                    open
                    onClose={() => {}}
                    privileges={[{ type: 'view', name: 'View' }]}
                />
            </BrowserRouter>,
        );

        // Check that "Containers" section is rendered
        expect(screen.getByText('Containers')).toBeInTheDocument();
    });

    it('renders ownership types correctly', () => {
        render(
            <BrowserRouter>
                <PolicyDetailsModal
                    policy={mockPolicyWithResourceOwners}
                    open
                    onClose={() => {}}
                    privileges={[{ type: 'view', name: 'View' }]}
                />
            </BrowserRouter>,
        );

        // Check the ownership types section
        expect(screen.getByText('Applies to Owners')).toBeInTheDocument();
        expect(screen.getByText('Technical Owner')).toBeInTheDocument();
    });
});
