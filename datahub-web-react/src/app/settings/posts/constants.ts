import { PostContentType } from '../../../types.generated';

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
