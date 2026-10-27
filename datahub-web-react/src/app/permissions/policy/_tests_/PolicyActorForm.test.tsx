import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';

import PolicyActorForm from '@app/permissions/policy/PolicyActorForm';
import themeV2 from '@conf/theme/themeV2';

import { ActorFilter, CorpUser, EntityType, PolicyType } from '@types';

// Avoid ActorPill's deep dependencies (Link, entity registry, tooltip) — we only
// need a clickable affordance that invokes the onClose handler the form wires up.
vi.mock('@app/sharedV2/owners/ActorPill', () => ({
    default: ({ actor, onClose }: any) => (
        <button type="button" data-testid={`remove-user-${actor?.urn}`} onClick={onClose}>
            {actor?.urn}
        </button>
    ),
}));

// The form unconditionally lists ownership types; stub the query-backed hook.
vi.mock('@app/sharedV2/owners/useOwnershipTypes', () => ({
    useOwnershipTypes: () => ({ data: { listOwnershipTypes: { ownershipTypes: [] } } }),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getDisplayName: () => 'Test User',
        getEntityUrl: () => '/user/test',
    }),
}));

const aliceUser: CorpUser = {
    urn: 'urn:li:corpuser:alice',
    type: EntityType.CorpUser,
    username: 'alice',
};

// Mock UsersSelect to ensure selected users are always rendered (bypassing SimpleSelect complexity)
vi.mock('@app/permissions/policy/PolicyActorForm/UsersSelect', () => ({
    default: ({ usersSelectValues, onDeselectUserActor }: any) => (
        <div data-testid="users">
            {usersSelectValues?.map((user: CorpUser) => (
                <button
                    key={user.urn}
                    type="button"
                    data-testid={`remove-user-${user.urn}`}
                    onClick={() => onDeselectUserActor(user.urn)}
                >
                    {user.username || user.urn}
                </button>
            ))}
        </div>
    ),
}));

vi.mock('@app/permissions/policy/PolicyActorForm/GroupsSelect', () => ({
    default: () => <div data-testid="groups" />,
}));

const renderForm = (actors: ActorFilter, setActors = vi.fn()) => {
    render(
        <MockedProvider mocks={[]} addTypename={false}>
            <ThemeProvider theme={themeV2}>
                <BrowserRouter>
                    <PolicyActorForm policyType={PolicyType.Platform} actors={actors} setActors={setActors} />
                </BrowserRouter>
            </ThemeProvider>
        </MockedProvider>,
    );
    return setActors;
};

describe('PolicyActorForm', () => {
    afterEach(() => {
        vi.clearAllMocks();
    });

    it('removes a deselected user from both users and resolvedUsers', () => {
        const { urn } = aliceUser;
        const actors = {
            users: [urn],
            resolvedUsers: [aliceUser],
            groups: [],
            resolvedGroups: [],
            allUsers: false,
            allGroups: false,
            resourceOwners: false,
            resourceOwnersTypes: null,
        } as unknown as ActorFilter;

        const setActors = renderForm(actors);

        // The form should render the selected user via ActorPill
        expect(screen.getByTestId(`remove-user-${urn}`)).toBeInTheDocument();

        fireEvent.click(screen.getByTestId(`remove-user-${urn}`));

        // Verify setActors was called once with updated state
        expect(setActors).toHaveBeenCalledTimes(1);
        const updated = setActors.mock.calls[0][0];
        expect(updated.users).toEqual([]);
        expect(updated.resolvedUsers).toEqual([]);
    });
});
