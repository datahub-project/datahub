import { Domain, Entity, EntityType } from '@src/types.generated';

/**
 * Type guard for domains
 */
export function isDomain(entity?: Entity | null | undefined): entity is Domain {
    return !!entity && entity.type === EntityType.Domain;
}
