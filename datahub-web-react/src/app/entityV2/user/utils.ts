import { CorpUser, Entity, EntityType } from '@src/types.generated';

/**
 * Type guard for users
 */
export function isItCorpUserEntity(entity?: Entity | null | undefined): entity is CorpUser {
    return !!entity && entity.type === EntityType.CorpUser;
}
