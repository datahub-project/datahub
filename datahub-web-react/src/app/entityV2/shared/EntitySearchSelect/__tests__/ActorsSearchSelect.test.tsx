import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { useUserContext } from '@app/context/useUserContext';
import { ActorsSearchSelect } from '@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect';
import { ActorEntity } from '@app/entityV2/shared/utils/actorUtils';
import { useGetRecommendations } from '@app/shared/recommendation';
import { useGetEntities } from '@app/sharedV2/useGetEntities';
import { act, fireEvent, render, screen, waitFor, within } from '@utils/test-utils/customRender';

import { useGetAutoCompleteMultipleResultsLazyQuery } from '@graphql/search.generated';
import { useGetUserGroupsQuery } from '@graphql/user.generated';
import { CorpGroup, CorpUser, Entity, EntityType } from '@types';

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

vi.mock('@app/shared/recommendation', () => ({
    useGetRecommendations: vi.fn(),
}));

vi.mock('@app/sharedV2/useGetEntities', () => ({
    useGetEntities: vi.fn(),
}));

vi.mock('@graphql/search.generated', () => ({
    useGetAutoCompleteMultipleResultsLazyQuery: vi.fn(),
}));

vi.mock('@graphql/user.generated', () => ({
    useGetUserGroupsQuery: vi.fn(),
}));

vi.mock('@src/app/useEntityRegistry', () => ({
    useEntityRegistryV2: () => ({
        getGenericEntityProperties: (_type: EntityType, entity: Entity) => entity,
        getIcon: () => null,
        getDisplayName: (_type: EntityType, entity: ActorEntity) => {
            if (entity.type === EntityType.CorpUser) {
                const user = entity as CorpUser;
                return (
                    user.properties?.displayName ||
                    user.info?.displayName ||
                    user.properties?.fullName ||
                    user.info?.fullName ||
                    user.username
                );
            }
            const group = entity as CorpGroup;
            return group.properties?.displayName || group.info?.displayName || group.name;
        },
    }),
}));

const mockUseUserContext = vi.mocked(useUserContext);
const mockUseGetRecommendations = vi.mocked(useGetRecommendations);
const mockUseGetEntities = vi.mocked(useGetEntities);
const mockUseGetAutoCompleteMultipleResultsLazyQuery = vi.mocked(useGetAutoCompleteMultipleResultsLazyQuery);
const mockUseGetUserGroupsQuery = vi.mocked(useGetUserGroupsQuery);

const CURRENT_USER = {
    urn: 'urn:li:corpuser:ada',
    type: EntityType.CorpUser,
    username: 'ada',
    properties: {
        displayName: 'Ada Lovelace',
        email: 'ada@example.com',
    },
    editableProperties: {},
} as CorpUser;

const SELECTED_USER = {
    urn: 'urn:li:corpuser:grace',
    type: EntityType.CorpUser,
    username: 'grace',
    properties: {
        displayName: 'Grace Hopper',
        email: 'grace@example.com',
    },
    editableProperties: {},
} as CorpUser;

const SEARCH_USER = {
    urn: 'urn:li:corpuser:katherine',
    type: EntityType.CorpUser,
    username: 'katherine',
    properties: {
        displayName: 'Katherine Johnson',
        email: 'katherine@example.com',
    },
    editableProperties: {},
} as CorpUser;

const CURRENT_USER_GROUP = {
    urn: 'urn:li:corpGroup:analytics',
    type: EntityType.CorpGroup,
    name: 'analytics',
    properties: {
        displayName: 'Analytics Team',
    },
} as CorpGroup;

const ENTITY_OWNER = {
    urn: 'urn:li:corpuser:mary',
    type: EntityType.CorpUser,
    username: 'mary',
    properties: {
        displayName: 'Mary Jackson',
        email: 'mary@example.com',
    },
    editableProperties: {},
} as CorpUser;

const ENTITY_WITH_OWNERS = {
    urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,table,PROD)',
    type: EntityType.Dataset,
    ownership: {
        owners: [{ owner: ENTITY_OWNER }],
    },
} as unknown as Entity;

function openDropdown() {
    fireEvent.click(screen.getByTestId('owners-select-base'));
    return screen.getByTestId('owners-select-dropdown');
}

function ControlledActorsSearchSelect() {
    const [selectedActorUrns, setSelectedActorUrns] = React.useState<string[]>([CURRENT_USER.urn]);

    return (
        <ActorsSearchSelect
            selectedActorUrns={selectedActorUrns}
            onUpdate={(actors) => setSelectedActorUrns(actors.map((actor) => actor.urn))}
            dataTestId="owners-select"
        />
    );
}

describe('ActorsSearchSelect', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        vi.stubGlobal(
            'IntersectionObserver',
            vi.fn((callback: IntersectionObserverCallback) => {
                const observer = {
                    observe: vi.fn((element: Element) => {
                        callback(
                            [{ isIntersecting: true, target: element } as IntersectionObserverEntry],
                            observer as IntersectionObserver,
                        );
                    }),
                    unobserve: vi.fn(),
                    disconnect: vi.fn(),
                    root: null,
                    rootMargin: '',
                    thresholds: [],
                    takeRecords: () => [],
                };
                return observer;
            }),
        );

        mockUseUserContext.mockReturnValue({
            loaded: true,
            user: CURRENT_USER,
        } as ReturnType<typeof useUserContext>);
        mockUseGetRecommendations.mockReturnValue({ recommendedData: [], loading: false });
        mockUseGetEntities.mockReturnValue({ entities: [], loading: false });
        mockUseGetUserGroupsQuery.mockReturnValue({
            data: {
                corpUser: {
                    relationships: {
                        relationships: [{ entity: CURRENT_USER_GROUP }],
                    },
                },
            },
            loading: false,
        } as any);
        mockUseGetAutoCompleteMultipleResultsLazyQuery.mockReturnValue([
            vi.fn(),
            { data: undefined, loading: false },
        ] as unknown as ReturnType<typeof useGetAutoCompleteMultipleResultsLazyQuery>);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        vi.useRealTimers();
    });

    it('includes the current user without selecting them', async () => {
        const onUpdate = vi.fn();

        render(<ActorsSearchSelect selectedActorUrns={[]} onUpdate={onUpdate} dataTestId="owners-select" />);

        const dropdown = openDropdown();

        await waitFor(() => expect(within(dropdown).getByText('Ada Lovelace')).toBeInTheDocument());
        expect(onUpdate).not.toHaveBeenCalled();
    });

    it('includes the current user groups when groups are allowed', async () => {
        const onUpdate = vi.fn();

        render(<ActorsSearchSelect selectedActorUrns={[]} onUpdate={onUpdate} dataTestId="owners-select" />);

        const dropdown = openDropdown();

        await waitFor(() => expect(within(dropdown).getByText('Analytics Team')).toBeInTheDocument());
    });

    it('includes owners for the provided entity urn', async () => {
        const onUpdate = vi.fn();

        mockUseGetEntities.mockReturnValue({ entities: [ENTITY_WITH_OWNERS], loading: false });

        render(
            <ActorsSearchSelect
                selectedActorUrns={[]}
                onUpdate={onUpdate}
                entityUrn={ENTITY_WITH_OWNERS.urn}
                dataTestId="owners-select"
            />,
        );

        const dropdown = openDropdown();

        await waitFor(() => expect(within(dropdown).getByText('Mary Jackson')).toBeInTheDocument());
    });

    it('renders a selected actor after hydrating the selected urn', async () => {
        const onUpdate = vi.fn();

        mockUseGetEntities.mockReturnValue({ entities: [SELECTED_USER], loading: false });

        render(
            <ActorsSearchSelect
                selectedActorUrns={[SELECTED_USER.urn]}
                onUpdate={onUpdate}
                dataTestId="owners-select"
            />,
        );

        await waitFor(() => expect(screen.getByText('Grace Hopper')).toBeInTheDocument());
    });

    it('keeps selected actors first in the dropdown', async () => {
        const onUpdate = vi.fn();

        mockUseGetEntities.mockReturnValue({ entities: [SELECTED_USER], loading: false });

        render(
            <ActorsSearchSelect
                selectedActorUrns={[SELECTED_USER.urn]}
                onUpdate={onUpdate}
                dataTestId="owners-select"
            />,
        );

        const dropdown = openDropdown();
        const options = await within(dropdown).findAllByTestId(/option-/);

        expect(options[0]).toHaveAttribute('data-testid', `option-${SELECTED_USER.urn}`);
    });

    it('shows typed search results while keeping selected actors renderable once the user searches', async () => {
        const onUpdate = vi.fn();

        vi.useFakeTimers();
        mockUseGetEntities.mockReturnValue({ entities: [SELECTED_USER], loading: false });
        mockUseGetRecommendations.mockReturnValue({ recommendedData: [CURRENT_USER], loading: false });
        mockUseGetAutoCompleteMultipleResultsLazyQuery.mockReturnValue([
            vi.fn(),
            {
                data: {
                    autoCompleteForMultiple: {
                        query: 'katherine',
                        suggestions: [{ entities: [SEARCH_USER] }],
                    },
                },
                loading: false,
            },
        ] as unknown as ReturnType<typeof useGetAutoCompleteMultipleResultsLazyQuery>);

        render(
            <ActorsSearchSelect
                selectedActorUrns={[SELECTED_USER.urn]}
                onUpdate={onUpdate}
                dataTestId="owners-select"
            />,
        );

        const dropdown = openDropdown();
        fireEvent.change(screen.getByTestId('dropdown-search-input'), { target: { value: 'katherine' } });

        await act(async () => {
            vi.advanceTimersByTime(300);
        });
        vi.useRealTimers();

        await waitFor(() => expect(within(dropdown).getByText('Katherine Johnson')).toBeInTheDocument());
        expect(screen.getByText('Grace Hopper')).toBeInTheDocument();
        expect(within(dropdown).queryByText('Grace Hopper')).not.toBeInTheDocument();
        expect(within(dropdown).queryByText('Ada Lovelace')).not.toBeInTheDocument();
    });

    it('clears a selected actor without reselecting defaults', async () => {
        mockUseGetEntities.mockReturnValue({ entities: [CURRENT_USER], loading: false });

        render(<ControlledActorsSearchSelect />);

        await waitFor(() => expect(screen.getByText('Ada Lovelace')).toBeInTheDocument());

        fireEvent.click(screen.getByTestId('button-clear'));

        await waitFor(() => expect(screen.queryByText('Ada Lovelace')).not.toBeInTheDocument());
        expect(screen.getByText('Search for users or groups')).toBeInTheDocument();
    });
});
