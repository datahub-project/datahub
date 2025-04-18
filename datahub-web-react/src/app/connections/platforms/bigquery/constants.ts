/*
 * Fields and configs are imported from ingest
 */
import { RECIPE_FIELDS } from '@app/ingest/source/builder/RecipeForm/constants';
import config, { BIGQUERY } from '@app/ingest/source/conf/bigquery/bigquery';

export const PLATFORM_NAME = 'BigQuery';
export const PLATFORM_URN = 'urn:li:dataPlatform:bigquery';

export const TEST_TYPE = 'bigquery';

export const FIELDS = RECIPE_FIELDS[BIGQUERY].fields;
export const CONFIG = config;
