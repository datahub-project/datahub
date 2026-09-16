import { isCorpGroup } from '@app/entityV2/group/utils';
import { isCorpUser } from '@app/entityV2/user/utils';
import { CorpGroup, CorpUser, Entity } from '@src/types.generated';

/**
 * Type definition for actor entities (CorpUser or CorpGroup)
 */
export type ActorEntity = CorpUser | CorpGroup;

/**
 * Type guard to check if an entity is an actor (CorpUser or CorpGroup)
 */
export function isActor(entity?: Entity | null | undefined): entity is ActorEntity {
    return isCorpUser(entity) || isCorpGroup(entity);
}

/**
 * Safely filters entities to only include actors (CorpUser or CorpGroup)
 * Returns a properly typed array without any hard casts
 */
export function filterActors(entities: (Entity | null | undefined)[]): ActorEntity[] {
    return entities.filter(isActor);
}

/**
 * Safely finds an actor entity by URN from a list of entities
 * Returns properly typed result without hard casts
 */
export function findActorByUrn(entities: Entity[], urn: string): ActorEntity | undefined {
    const entity = entities.find((e) => e.urn === urn);
    return isActor(entity) ? entity : undefined;
}

/**
 * Resolves actor entities from URNs using multiple data sources
 * Returns properly typed array without hard casts
 */
export function resolveActorsFromUrns(
    urns: string[],
    sources: {
        placeholderActors?: ActorEntity[];
        searchResults?: Entity[];
        selectedActors?: ActorEntity[];
    },
): ActorEntity[] {
    const { placeholderActors = [], searchResults = [], selectedActors = [] } = sources;

    return urns
        .map((urn) => {
            // Try to find in placeholder actors first (already typed)
            const fromPlaceholders = placeholderActors.find((e) => e.urn === urn);
            if (fromPlaceholders) return fromPlaceholders;

            // Try to find in search results (need type checking)
            const fromSearch = findActorByUrn(searchResults, urn);
            if (fromSearch) return fromSearch;

            // Try to find in selected actors (already typed)
            const fromSelected = selectedActors.find((e) => e.urn === urn);
            if (fromSelected) return fromSelected;

            return undefined;
        })
        .filter((entity): entity is ActorEntity => entity !== undefined);
}

/**
 * Checks if an entity has the properties expected for actor display
 */
export function hasActorDisplayProperties(entity: Entity): boolean {
    return isActor(entity) && Boolean(entity.urn && entity.type);
}

/**
 * Type-safe way to get display properties from an actor entity
 */
export function getActorDisplayName(entity: ActorEntity): string | undefined {
    if (isCorpUser(entity)) {
        return entity.properties?.displayName || entity.properties?.fullName || entity.username;
    }
    if (isCorpGroup(entity)) {
        return entity.properties?.displayName || entity.name;
    }
    return undefined;
}

/**
 * Type-safe way to get email from an actor (CorpUser only)
 */
export function getActorEmail(entity: ActorEntity): string | undefined {
    return isCorpUser(entity) ? entity.properties?.email || undefined : undefined;
}

/**
 * Type-safe way to get picture link from an actor
 */
export function getActorPictureLink(entity: ActorEntity): string | undefined {
    return entity.editableProperties?.pictureLink || undefined;
}
