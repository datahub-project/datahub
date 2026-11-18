import { MockedProvider, MockedResponse } from '@apollo/client/testing';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { GraphQLError } from 'graphql';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it, vi } from 'vitest';

import { ActorPoliciesTab } from '@app/permissions/actorPolicies/ActorPoliciesTab';
import { ActorPolicyReasonPills } from '@app/permissions/actorPolicies/ActorPolicyReasonPills';
import themeV2 from '@conf/theme/themeV2';

import { GetUserPoliciesDocument } from '@graphql/actorPolicies.generated';
import { GetPolicyPrivilegesDocument } from '@graphql/app.generated';
import { ActorPoliciesView, EntityType, PolicyMatchReasonType, PolicyState, PolicyType } from '@types';

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getEntityUrl: (_type: unknown, urn: string) => `/group/${urn}`,
        getDisplayName: (_type: unknown, entity: { name?: string }) => entity?.name ?? '',
        hasEntity: () => true,
        getGenericEntityProperties: () => null,
        getIcon: () => null,
    }),
}));

vi.mock('@app/permissions/policy/PolicyDetailsModal', () => ({
    default: ({ policy }: { policy: { name: string } }) => <div data-testid="details-modal">{policy.name}</div>,
}));

const URN = 'urn:li:corpuser:me';

const policy = (overrides = {}) => ({
    __typename: 'ActorPolicy',
    urn: 'urn:li:dataHubPolicy:1',
    builtIn: false,
    name: 'Edit Policy',
    description: null,
    type: PolicyType.Metadata,
    state: PolicyState.Active,
    privileges: ['EDIT_ENTITY_TAGS'],
    resources: null,
    actors: null,
    matchedBy: [
        { type: PolicyMatchReasonType.DirectUser, group: null, role: null },
        {
            type: PolicyMatchReasonType.Group,
            group: {
                __typename: 'CorpGroup',
                urn: 'urn:li:corpGroup:g',
                type: EntityType.CorpGroup,
                name: 'Eng',
                properties: null,
            },
            role: null,
        },
    ],
    ...overrides,
});

const privilegesMock: MockedResponse = {
    request: { query: GetPolicyPrivilegesDocument },
    result: {
        data: {
            appConfig: {
                policiesConfig: {
                    platformPrivileges: [],
                    resourcePrivileges: [
                        {
                            resourceType: 'all',
                            resourceTypeDisplayName: 'All',
                            entityType: null,
                            privileges: [{ type: 'EDIT_ENTITY_TAGS', displayName: 'Edit Tags', description: '' }],
                        },
                    ],
                },
            },
        },
    },
};

const policiesMock = (view: ActorPoliciesView, policies: unknown[], ownerPolicies: unknown[] = []): MockedResponse => ({
    request: { query: GetUserPoliciesDocument, variables: { urn: URN, includeInactive: false } },
    result: { data: { corpUser: { urn: URN, policies: { view, policies, ownerPolicies } } } },
});

const renderTab = (mocks: MockedResponse[]) =>
    render(
        <MockedProvider mocks={[...mocks, privilegesMock]} addTypename={false}>
            <ThemeProvider theme={themeV2}>
                <BrowserRouter>
                    <ActorPoliciesTab urn={URN} entityType={EntityType.CorpUser} />
                </BrowserRouter>
            </ThemeProvider>
        </MockedProvider>,
    );

describe('ActorPoliciesTab', () => {
    it('renders full columns and opens details on row click', async () => {
        renderTab([policiesMock(ActorPoliciesView.Full, [policy()])]);
        expect(await screen.findByText('Edit Policy')).toBeInTheDocument();
        expect(screen.getByText('Applies because')).toBeInTheDocument();
        expect(screen.getByText('Resources')).toBeInTheDocument();
        expect(screen.getByTestId('actor-policy-status-filter')).toBeInTheDocument();
        await userEvent.click(screen.getByText('Edit Policy'));
        expect(screen.getByTestId('details-modal')).toHaveTextContent('Edit Policy');
    });

    it('trimmed view hides actors, status filter and click-through', async () => {
        renderTab([policiesMock(ActorPoliciesView.Trimmed, [policy()])]);
        expect(await screen.findByText('Edit Policy')).toBeInTheDocument();
        expect(screen.getByText('Lets you')).toBeInTheDocument();
        expect(screen.queryByText('State')).not.toBeInTheDocument();
        expect(screen.queryByTestId('actor-policy-status-filter')).not.toBeInTheDocument();
        await userEvent.click(screen.getByText('Edit Policy'));
        expect(screen.queryByTestId('details-modal')).not.toBeInTheDocument();
    });

    it('built-in row is not clickable and shows Built-in', async () => {
        renderTab([policiesMock(ActorPoliciesView.Full, [policy({ urn: null, builtIn: true, name: 'View Self' })])]);
        expect(await screen.findByText('View Self')).toBeInTheDocument();
        expect(screen.getByText('Built-in')).toBeInTheDocument();
        await userEvent.click(screen.getByText('View Self'));
        expect(screen.queryByTestId('details-modal')).not.toBeInTheDocument();
    });

    it('lists owner policies in a separate section', async () => {
        renderTab([
            policiesMock(
                ActorPoliciesView.Trimmed,
                [policy()],
                [policy({ urn: 'urn:li:dataHubPolicy:2', name: 'Owner Policy' })],
            ),
        ]);
        expect(await screen.findByText('Applies to assets you own')).toBeInTheDocument();
        expect(screen.getByText('Owner Policy')).toBeInTheDocument();
    });

    it('unauthorized renders empty state', async () => {
        renderTab([
            {
                request: { query: GetUserPoliciesDocument, variables: { urn: URN, includeInactive: false } },
                result: { errors: [{ message: 'nope', extensions: { code: 403 } } as unknown as GraphQLError] },
            },
        ]);
        await waitFor(() => expect(screen.getByText('No policies apply.')).toBeInTheDocument());
    });
});

describe('ActorPolicyReasonPills', () => {
    it('reason pills render group link and role name', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <BrowserRouter>
                    <ActorPolicyReasonPills
                        reasons={[
                            {
                                type: PolicyMatchReasonType.Group,
                                group: { urn: 'urn:li:corpGroup:g', type: EntityType.CorpGroup, name: 'Eng' },
                                role: null,
                            },
                            {
                                type: PolicyMatchReasonType.Role,
                                group: null,
                                role: { urn: 'urn:li:dataHubRole:r', type: EntityType.DatahubRole, name: 'Editor' },
                            },
                        ]}
                    />
                </BrowserRouter>
            </ThemeProvider>,
        );
        expect(screen.getByText('Group: Eng').closest('a')).toHaveAttribute('href', '/group/urn:li:corpGroup:g');
        expect(screen.getByText('Role: Editor')).toBeInTheDocument();
    });
});
