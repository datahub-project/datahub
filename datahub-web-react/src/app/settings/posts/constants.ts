/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { PostContentType } from '@types';

export const TITLE_FIELD_NAME = 'title';
export const DESCRIPTION_FIELD_NAME = 'description';
export const LINK_FIELD_NAME = 'link';
export const LOCATION_FIELD_NAME = 'location';
export const TYPE_FIELD_NAME = 'type';
export const CREATE_POST_BUTTON_ID = 'createPostButton';

export const POST_TYPE_TO_DISPLAY_TEXT = {
    [PostContentType.Link]: 'Link',
    [PostContentType.Text]: 'Announcement',
};
