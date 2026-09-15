import { useMemo } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { ActorEntity, filterActors } from '@app/entityV2/shared/utils/actorUtils';
import { useGetRecommendations } from '@app/shared/recommendation';
import { useGetEntities } from '@app/sharedV2/useGetEntities';

import { useGetUserGroupsQuery } from '@graphql/user.generated';
import { Entity, EntityType } from '@types';

const MAX_GROUP_RECOMMENDATIONS = 10;

interface UseActorOptionsProps {
    selectedActorUrns: string[];
    entityTypes: EntityType[];
    entityUrn?: string;
    includeCurrentUser: boolean;
    includeCurrentUserGroups: boolean;
    searchResults: Entity[];
    isSearching: boolean;
}

function allowsEntityType(entityTypes: EntityType[], entityType: EntityType) {
    return entityTypes.includes(entityType);
}

function getEntityOwners(entity?: Entity): ActorEntity[] {
    const owners = (entity as any)?.ownership?.owners?.map((owner) => owner.owner) || [];
    return filterActors(owners);
}

function deduplicateActors(actorSources: ActorEntity[][]): ActorEntity[] {
    const seenUrns = new Set<string>();
    return actorSources.flat().filter((actor) => {
        if (seenUrns.has(actor.urn)) return false;
        seenUrns.add(actor.urn);
        return true;
    });
}

function getSelectedActorsInOrder(selectedActorUrns: string[], actorSources: ActorEntity[][]): ActorEntity[] {
    const actorsByUrn = new Map<string, ActorEntity>();
    actorSources.flat().forEach((actor) => {
        if (!actorsByUrn.has(actor.urn)) {
            actorsByUrn.set(actor.urn, actor);
        }
    });

    return selectedActorUrns.map((urn) => actorsByUrn.get(urn)).filter((actor): actor is ActorEntity => !!actor);
}

export function useActorOptions({
    selectedActorUrns,
    entityTypes,
    entityUrn,
    includeCurrentUser,
    includeCurrentUserGroups,
    searchResults,
    isSearching,
}: UseActorOptionsProps) {
    const { user } = useUserContext();
    const shouldIncludeCurrentUser = includeCurrentUser && allowsEntityType(entityTypes, EntityType.CorpUser);
    const shouldIncludeCurrentUserGroups =
        includeCurrentUserGroups && allowsEntityType(entityTypes, EntityType.CorpGroup) && !!user?.urn;

    const { data: userGroupsData, loading: userGroupsLoading } = useGetUserGroupsQuery({
        variables: {
            urn: user?.urn || '',
            start: 0,
            count: MAX_GROUP_RECOMMENDATIONS,
        },
        skip: !shouldIncludeCurrentUserGroups,
        fetchPolicy: 'cache-first',
    });

    const urnsToHydrate = useMemo(
        () => Array.from(new Set([...selectedActorUrns, ...(entityUrn ? [entityUrn] : [])])),
        [entityUrn, selectedActorUrns],
    );
    const { entities: hydratedEntities, loading: hydratedEntitiesLoading } = useGetEntities(urnsToHydrate);
    const { recommendedData, loading: recommendationsLoading } = useGetRecommendations(entityTypes);

    const currentUserActors = useMemo(
        () => (shouldIncludeCurrentUser ? filterActors([user]) : []),
        [shouldIncludeCurrentUser, user],
    );

    const currentUserGroupActors = useMemo(() => {
        if (!shouldIncludeCurrentUserGroups) return [];
        const groups =
            userGroupsData?.corpUser?.relationships?.relationships
                ?.map((relationship) => relationship.entity)
                .filter((entity): entity is Entity => !!entity) || [];
        return filterActors(groups);
    }, [shouldIncludeCurrentUserGroups, userGroupsData]);

    const hydratedSelectedActors = useMemo(() => {
        const selectedUrns = new Set(selectedActorUrns);
        return filterActors(hydratedEntities.filter((entity) => selectedUrns.has(entity.urn)));
    }, [hydratedEntities, selectedActorUrns]);

    const entityOwnerActors = useMemo(() => {
        if (!entityUrn) return [];
        const entity = hydratedEntities.find((hydratedEntity) => hydratedEntity.urn === entityUrn);
        return getEntityOwners(entity);
    }, [entityUrn, hydratedEntities]);

    const initialSearchActors = useMemo(() => filterActors(recommendedData), [recommendedData]);
    const typedSearchActors = useMemo(() => filterActors(searchResults), [searchResults]);

    const selectedActors = useMemo(
        () =>
            getSelectedActorsInOrder(selectedActorUrns, [
                hydratedSelectedActors,
                currentUserActors,
                currentUserGroupActors,
                entityOwnerActors,
                typedSearchActors,
                initialSearchActors,
            ]),
        [
            selectedActorUrns,
            hydratedSelectedActors,
            currentUserActors,
            currentUserGroupActors,
            entityOwnerActors,
            typedSearchActors,
            initialSearchActors,
        ],
    );

    const actorOptions = useMemo(() => {
        if (isSearching) {
            return deduplicateActors([typedSearchActors]);
        }
        return deduplicateActors([
            selectedActors,
            currentUserActors,
            currentUserGroupActors,
            entityOwnerActors,
            initialSearchActors,
        ]);
    }, [
        currentUserActors,
        currentUserGroupActors,
        entityOwnerActors,
        initialSearchActors,
        isSearching,
        selectedActors,
        typedSearchActors,
    ]);

    return {
        actorOptions,
        selectedActors,
        loading: hydratedEntitiesLoading || userGroupsLoading || (!isSearching && recommendationsLoading),
    };
}
