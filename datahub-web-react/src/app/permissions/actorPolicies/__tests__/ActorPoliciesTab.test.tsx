import { MockedProvider, MockedResponse } from '@apollo/client/testing';
import { render, screen, waitFor, within } from '@testing-library/react';
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
                    platformPrivileges: [{ type: 'MANAGE_USERS', displayName: 'Manage Users', description: '' }],
                    resourcePrivileges: [
                        {
                            resourceType: 'all',
                            resourceTypeDisplayName: 'All',
                            entityType: null,
                            privileges: [{ type: 'EDIT_ENTITY_TAGS', displayName: 'Edit Tags', description: '' }],
                        },
                        {
                            resourceType: 'dataset',
                            resourceTypeDisplayName: 'Dataset',
                            entityType: EntityType.Dataset,
                            privileges: [],
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

    it('shows the manage policies link in FULL view only', async () => {
        const { unmount } = renderTab([policiesMock(ActorPoliciesView.Full, [policy()])]);
        expect(await screen.findByText('Manage policies')).toHaveAttribute('href', '/settings/policies');
        unmount();
        renderTab([policiesMock(ActorPoliciesView.Trimmed, [policy()])]);
        await screen.findByText('Edit Policy');
        expect(screen.queryByText('Manage policies')).not.toBeInTheDocument();
    });

    it('describes the resource scope of each policy', async () => {
        const criterion = (field: string, values: unknown[]) => ({
            field,
            values,
            condition: 'EQUALS',
            structuredPropertyValues: null,
        });
        renderTab([
            policiesMock(ActorPoliciesView.Trimmed, [
                policy({
                    urn: 'urn:li:dataHubPolicy:t',
                    name: 'Typed',
                    resources: { filter: { criteria: [criterion('TYPE', [{ value: 'dataset', entity: null }])] } },
                }),
                policy({
                    urn: 'urn:li:dataHubPolicy:d',
                    name: 'Scoped',
                    resources: {
                        filter: {
                            criteria: [
                                criterion('DOMAIN', [
                                    {
                                        value: 'urn:li:domain:d',
                                        entity: {
                                            __typename: 'Domain',
                                            urn: 'urn:li:domain:d',
                                            type: EntityType.Domain,
                                            name: 'Finance',
                                        },
                                    },
                                ]),
                            ],
                        },
                    },
                }),
                policy({ urn: 'urn:li:dataHubPolicy:a', name: 'Everything', resources: { filter: { criteria: [] } } }),
                policy({
                    urn: 'urn:li:dataHubPolicy:p',
                    name: 'Platform',
                    type: PolicyType.Platform,
                    privileges: ['MANAGE_USERS'],
                    resources: null,
                }),
            ]),
        ]);
        await screen.findByText('Typed');
        const rowOf = (name: string) => screen.getByText(name).closest('tr') as HTMLElement;
        expect(within(rowOf('Typed')).getByText('Dataset')).toBeInTheDocument();
        expect(within(rowOf('Scoped')).getByText('Finance')).toBeInTheDocument();
        expect(within(rowOf('Everything')).getByText('All')).toBeInTheDocument();
        expect(within(rowOf('Platform')).getByText('—')).toBeInTheDocument();
        expect(within(rowOf('Platform')).queryByText('All')).not.toBeInTheDocument();
    });

    it('non-403 errors show an error, not the empty state', async () => {
        renderTab([
            {
                request: { query: GetUserPoliciesDocument, variables: { urn: URN, includeInactive: false } },
                result: { errors: [{ message: 'boom', extensions: { code: 500 } } as unknown as GraphQLError] },
            },
        ]);
        expect(await screen.findByTestId('actor-policies-error')).toBeInTheDocument();
        expect(screen.queryByText('No policies apply.')).not.toBeInTheDocument();
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
