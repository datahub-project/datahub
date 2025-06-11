/*
 * Fields and configs are imported from ingest
 */
import { RECIPE_FIELDS } from '@app/ingest/source/builder/RecipeForm/constants';
import { UNITY_CATALOG } from '@app/ingest/source/builder/constants';
import config from '@app/ingest/source/conf/unity_catalog/unity_catalog';

export const PLATFORM_NAME = 'Databricks';
export const PLATFORM_URN = 'urn:li:dataPlatform:databricks';

export const TEST_TYPE = UNITY_CATALOG;

export const FIELDS = RECIPE_FIELDS[UNITY_CATALOG].fields;
export const CONFIG = config;
