/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { EntityType } from '@types';

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

export const toTitleCase = (str: string): string => {
    // eslint-disable-next-line no-useless-escape
    return `${str.charAt(0).toUpperCase()}${str.substr(1)}`.replace(/[\-_]/g, '');
};
