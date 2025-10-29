import { EntityType } from '@types';

export const toTitleCase = (str: string): string => {
    // eslint-disable-next-line no-useless-escape
    return `${str.charAt(0).toUpperCase()}${str.substr(1)}`.replace(/[\-_]/g, '');
};
