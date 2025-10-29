import { EntityType } from '@types';

/**
 * Common helpers
 */

const getActor = (): string | null => {
    const cookie = new URLSearchParams(document.cookie.replaceAll('; ', '&'));
    return cookie.get('actor');
};

const toLowerCaseEntityType = (type: EntityType): string => {
    return type.toLowerCase().replace(/[_]/g, '');
};

export const toTitleCase = (str: string): string => {
    // eslint-disable-next-line no-useless-escape
    return `${str.charAt(0).toUpperCase()}${str.substr(1)}`.replace(/[\-_]/g, '');
};
