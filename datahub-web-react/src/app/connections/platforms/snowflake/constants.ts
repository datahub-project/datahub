/*
 * Fields and configs are imported from ingest
 */
import { RECIPE_FIELDS } from '@app/ingest/source/builder/RecipeForm/constants';
import config, { SNOWFLAKE } from '@app/ingest/source/conf/snowflake/snowflake';

export const PLATFORM_NAME = 'Snowflake';
export const PLATFORM_URN = 'urn:li:dataPlatform:snowflake';

export const TEST_TYPE = 'snowflake';

export const FIELDS = RECIPE_FIELDS[SNOWFLAKE].fields;
export const CONFIG = config;
