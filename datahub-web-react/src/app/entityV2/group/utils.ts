import { CorpGroup, Entity, EntityType } from '@src/types.generated';

/**
 * Type guard for groups
 */
export function isCorpGroup(entity?: Entity | null | undefined): entity is CorpGroup {
    return !!entity && entity.type === EntityType.CorpGroup;
}
