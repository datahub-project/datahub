import { Entity, EntityType, Tag } from '@src/types.generated';

/**
 * Type guard for tags
 */
export function isTag(entity?: Entity | null | undefined): entity is Tag {
    return !!entity && entity.type === EntityType.Tag;
}
