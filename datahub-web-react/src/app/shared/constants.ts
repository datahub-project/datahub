/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export const ENTER_KEY_CODE = 13;

export const STRING_TYPE_URN = 'urn:li:dataType:datahub.string';
export const NUMBER_TYPE_URN = 'urn:li:dataType:datahub.number';
export const URN_TYPE_URN = 'urn:li:dataType:datahub.urn';
export const RICH_TEXT_TYPE_URN = 'urn:li:dataType:datahub.rich_text';
export const DATE_TYPE_URN = 'urn:li:dataType:datahub.date';

export const TYPE_ICON_CLASS_NAME = 'typeIcon';

export enum ErrorCodes {
    BadRequest = 400,
    Unauthorized = 401,
    Forbidden = 403,
    NotFound = 404,
    ServerError = 500,
}

export const DEBOUNCE_SEARCH_MS = 300;

export const ANT_NOTIFICATION_Z_INDEX = 1010;

// S3 folder to store product assets
export const PRODUCT_ASSETS_FOLDER = 'product_assets';
