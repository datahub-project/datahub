/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { FieldType, RecipeField } from '@app/ingest/source/builder/RecipeForm/common';

export const SNOWFLAKE_ACCOUNT_ID: RecipeField = {
    name: 'account_id',
    label: 'Account ID',
    tooltip:
        'The Snowflake Account Identifier e.g. myorg-account123, account123-eu-central-1, account123.west-us-2.azure',
    type: FieldType.TEXT,
    fieldPath: 'source.config.account_id',
    placeholder: 'xyz123',
    rules: null,
    required: true,
};

export const SNOWFLAKE_WAREHOUSE: RecipeField = {
    name: 'warehouse',
    label: 'Warehouse',
    tooltip: 'The name of the Snowflake Warehouse to extract metadata from.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.warehouse',
    placeholder: 'COMPUTE_WH',
    rules: null,
    required: true,
};

export const SNOWFLAKE_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'Snowflake username.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'snowflake',
    rules: null,
    required: true,
};

export const SNOWFLAKE_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'Snowflake password.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    rules: null,
    required: true,
};

export const SNOWFLAKE_ROLE: RecipeField = {
    name: 'role',
    label: 'Role',
    tooltip: 'The Role to use when extracting metadata from Snowflake.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.role',
    placeholder: 'datahub_role',
    rules: null,
    required: true,
};
