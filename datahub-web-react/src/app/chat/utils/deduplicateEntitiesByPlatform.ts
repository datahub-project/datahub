import { Entity } from '@types';

/**
 * Deduplicates entities by their platform URN to avoid showing the same platform logo multiple times.
 * If an entity doesn't have a platform URN, it uses the entity URN as the key.
 *
 * @param entities - Array of entities to deduplicate
 * @returns Array of entities with unique platforms
 */
export function deduplicateEntitiesByPlatform(entities: Entity[]): Entity[] {
    const seenPlatforms = new Set<string>();

    return entities.filter((entity) => {
        const platformUrn = (entity as { platform?: { urn?: string } })?.platform?.urn;
        const key = platformUrn || entity.urn;

        if (seenPlatforms.has(key)) {
            return false;
        }

        seenPlatforms.add(key);
        return true;
    });
}
