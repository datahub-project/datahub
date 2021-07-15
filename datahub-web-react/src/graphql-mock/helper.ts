import { EntityType } from '../types.generated';

/**
 * Common helpers
 */

export const getActor = (): string | null => {
    const cookie = new URLSearchParams(document.cookie.replaceAll('; ', '&'));
    return cookie.get('actor');
};

export const toLowerCaseEntityType = (type: EntityType): string => {
    return type.toLowerCase().replace(/[_]/g, '');
};
