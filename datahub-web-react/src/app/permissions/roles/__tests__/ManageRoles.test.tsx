import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ManageRoles } from '@app/permissions/roles/ManageRoles';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

// useListRolesQuery is driven per-test; everything else keeps its real implementation.
const mockUseListRolesQuery = vi.hoisted(() => vi.fn());

vi.mock('@graphql/role.generated', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@graphql/role.generated')>();
    return { ...actual, useListRolesQuery: (args: any) => mockUseListRolesQuery(args) };
});

vi.mock('@graphql/mutations.generated', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@graphql/mutations.generated')>();
    return { ...actual, useBatchAssignRoleMutation: () => [vi.fn(), { client: {} }] };
});

// OnboardingTour pulls in unrelated onboarding context; stub it out for this render test.
vi.mock('@app/onboarding/OnboardingTour', () => ({ OnboardingTour: () => null }));

const PLATFORM_POLICY = {
    urn: 'urn:li:dataHubPolicy:admin-platform-policy',
    type: 'DATAHUB_POLICY',
    name: 'Platform Policy',
};
const METADATA_POLICY = {
    urn: 'urn:li:dataHubPolicy:admin-metadata-policy',
    type: 'DATAHUB_POLICY',
    name: 'Metadata Policy',
};

function adminRoleData(options: { policyRelationships?: any[]; userRelationships?: any[] }) {
    const policyRelationships = options.policyRelationships ?? [];
    const userRelationships = options.userRelationships ?? [];
    return {
        listRoles: {
            start: 0,
            count: 1,
            total: 1,
            roles: [
                {
                    urn: 'urn:li:dataHubRole:Admin',
                    type: 'DATAHUB_ROLE',
                    name: 'Admin',
                    description: 'Admin role',
                    users: {
                        start: 0,
                        count: userRelationships.length,
                        total: userRelationships.length,
                        relationships: userRelationships,
                    },
                    policies: {
                        start: 0,
                        count: policyRelationships.length,
                        total: policyRelationships.length,
                        relationships: policyRelationships,
                    },
                },
            ],
        },
    };
}

function mockListRoles(data: any) {
    mockUseListRolesQuery.mockReturnValue({ loading: false, error: undefined, data, refetch: vi.fn() });
}

function renderManageRoles() {
    // MockedProvider supplies the Apollo client that TestPageContainer's context providers require;
    // ManageRoles' own listRoles query is stubbed via the vi.mock above, so no GraphQL mocks are needed.
    return render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>
                <ManageRoles />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('ManageRoles', () => {
    beforeEach(() => {
        // The null-entity drop logs a defensive warning; silence it so it doesn't fail the run.
        vi.spyOn(console, 'warn').mockImplementation(() => {});
        mockUseListRolesQuery.mockReset();
    });

    // Reproduces the Settings → Roles crash: a globalSettings maintenance-window edge resolves to a
    // null entity under the `... on DataHubPolicy` fragment, sitting between two real policies.
    // Rendering it previously threw `TypeError: can't access property "urn", U is null` on the React key.
    it('renders without crashing when a policy relationship entity is null', () => {
        mockListRoles(
            adminRoleData({
                policyRelationships: [{ entity: PLATFORM_POLICY }, { entity: null }, { entity: METADATA_POLICY }],
            }),
        );

        renderManageRoles();

        // Both real policies render; the null edge is skipped rather than rendered as a broken chip.
        expect(screen.getByText('Platform Policy')).toBeInTheDocument();
        expect(screen.getByText('Metadata Policy')).toBeInTheDocument();
    });

    it('renders without crashing when every policy relationship entity is null', () => {
        mockListRoles(adminRoleData({ policyRelationships: [{ entity: null }, { entity: null }] }));

        renderManageRoles();

        expect(screen.getByText('Admin')).toBeInTheDocument();
    });

    // Defensive symmetry: a null member (users) edge must not crash the row either.
    it('renders without crashing when a member relationship entity is null', () => {
        mockListRoles(
            adminRoleData({
                policyRelationships: [{ entity: PLATFORM_POLICY }],
                userRelationships: [{ entity: { urn: 'urn:li:corpuser:alice', type: 'CORP_USER' } }, { entity: null }],
            }),
        );

        renderManageRoles();

        expect(screen.getByText('Platform Policy')).toBeInTheDocument();
    });
});
