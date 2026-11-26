import i18next from 'i18next';

import { PostContentType } from '@types';

const map: Record<string, string> = {};
Object.defineProperty(map, PostContentType.Link, {
    get: () => i18next.t('settings.posts:typeLink'),
    enumerable: true,
    configurable: true,
});
Object.defineProperty(map, PostContentType.Text, {
    get: () => i18next.t('settings.posts:typeAnnouncement'),
    enumerable: true,
    configurable: true,
});

export const POST_TYPE_TO_DISPLAY_TEXT: Record<string, string> = map;

export const LINK_FIELD_NAME = 'link';
export const LOCATION_FIELD_NAME = 'location';
export const TYPE_FIELD_NAME = 'type';
export const CREATE_POST_BUTTON_ID = 'createPostButton';
