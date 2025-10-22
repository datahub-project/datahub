/*
 * Fields and configs are imported from ingest
 */
import { RECIPE_FIELDS } from '@app/ingest/source/builder/RecipeForm/constants';
import { DATABRICKS } from '@app/ingest/source/builder/constants';
import config from '@app/ingest/source/conf/unity_catalog/unity_catalog';

export const PLATFORM_NAME = 'Databricks';
export const PLATFORM_URN = 'urn:li:dataPlatform:databricks';

export const TEST_TYPE = DATABRICKS;

export const FIELDS = RECIPE_FIELDS[DATABRICKS].fields;
export const CONFIG = config;
