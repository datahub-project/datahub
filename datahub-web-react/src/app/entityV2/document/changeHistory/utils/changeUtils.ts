import { EntityType, StringMapEntry } from '@types';

/**
 * URN for the DataHub system actor
 */
export const DATAHUB_SYSTEM_ACTOR_URN = 'urn:li:corpuser:__datahub_system';

/**
 * Checks if an actor is the DataHub system actor.
 *
 * @param actor - The actor to check
 * @returns true if the actor is the system actor, false otherwise
 */
export function isSystemActor(actor: { urn?: string } | null | undefined): boolean {
    return actor?.urn === DATAHUB_SYSTEM_ACTOR_URN;
}

/**
 * Converts an array of StringMapEntry objects into a key-value record.
 * This makes it easier to access change details by key.
 *
 * @param details - Array of StringMapEntry from the change history
 * @returns Record<string, string> with keys and values
 *
 * @example
 * const details = [
 *   { key: 'oldTitle', value: 'Old Title' },
 *   { key: 'newTitle', value: 'New Title' }
 * ];
 * const record = extractChangeDetails(details);
 * // { oldTitle: 'Old Title', newTitle: 'New Title' }
 */
export function extractChangeDetails(details?: StringMapEntry[] | null): Record<string, string> {
    if (!details || details.length === 0) {
        return {};
    }

    return details.reduce(
        (acc, detail) => {
            acc[detail.key] = detail.value || '';
            return acc;
        },
        {} as Record<string, string>,
    );
}

/**
 * Gets the display name for a change actor.
 * - Returns 'DataHub AI' for the system actor
 * - Returns 'System' if no actor is present
 * - Otherwise returns the actor's display name from the registry
 *
 * @param actor - The actor who made the change (optional)
 * @param entityRegistry - Entity registry instance with getDisplayName method
 * @returns The display name for the actor
 *
 * @example
 * const name = getActorDisplayName(change.actor, entityRegistry);
 * // Returns 'John Doe', 'DataHub AI', or 'System'
 */
export function getActorDisplayName(
    actor: { type: EntityType; urn?: string; [key: string]: any } | null | undefined,
    entityRegistry: { getDisplayName: (type: EntityType, data: any) => string },
): string {
    if (!actor) {
        return 'System';
    }

    if (isSystemActor(actor)) {
        return 'DataHub AI';
    }

    return entityRegistry.getDisplayName(actor.type, actor);
}
