import { get } from 'lodash';
import React from 'react';
import styled from 'styled-components';

import { FieldType, RecipeField } from '@app/ingest/source/builder/RecipeForm/common';

const TipSection = styled.div`
    margin-bottom: 12px;
`;

export const GOOGLE_SHEETS = 'google-sheets';

export const GOOGLE_SHEETS_CREDENTIALS: RecipeField = {
    name: 'credentials',
    label: 'Service Account Credentials',
    tooltip: (
        <span>
            <TipSection>
                Path to the Google service account credentials JSON file. This service account must have access to the
                Google Sheets and Google Drive APIs.
            </TipSection>
            <TipSection>
                Learn more about creating Google service accounts
                <a
                    href="https://developers.google.com/identity/protocols/oauth2/service-account#creatinganaccount"
                    target="_blank"
                    rel="noopener noreferrer"
                >
                    {' '}
                    here
                </a>
                .
            </TipSection>
        </span>
    ),
    type: FieldType.FILE,
    fieldPath: 'source.config.credentials',
    placeholder: '/path/to/credentials.json',
    required: true,
    rules: null,
};

export const GOOGLE_SHEETS_PLATFORM_INSTANCE: RecipeField = {
    name: 'platform_instance',
    label: 'Platform Instance',
    tooltip: 'The platform instance for the Google Sheets source, e.g., "prod" or "dev". This is optional.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.platform_instance',
    placeholder: 'prod',
    required: false,
    rules: null,
};

const extractUsageStatsPath = 'source.config.extract_usage_stats';
export const EXTRACT_USAGE_STATS: RecipeField = {
    name: 'extract_usage_stats',
    label: 'Extract Usage Statistics',
    tooltip: 'Whether to extract usage statistics for Google Sheets.',
    type: FieldType.BOOLEAN,
    fieldPath: extractUsageStatsPath,
    required: false,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const extractUsageStats = get(recipe, extractUsageStatsPath);
        if (extractUsageStats !== undefined && extractUsageStats !== null) {
            return extractUsageStats;
        }
        return true;
    },
};

const extractLineagePath = 'source.config.extract_lineage_from_formulas';
export const EXTRACT_LINEAGE: RecipeField = {
    name: 'extract_lineage_from_formulas',
    label: 'Extract Lineage from Formulas',
    tooltip:
        'Whether to extract lineage information from sheet formulas. This includes connections to other Google Sheets and external data sources.',
    type: FieldType.BOOLEAN,
    fieldPath: extractLineagePath,
    required: false,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const extractLineage = get(recipe, extractLineagePath);
        if (extractLineage !== undefined && extractLineage !== null) {
            return extractLineage;
        }
        return true;
    },
};

const enableCrossPlatformLineagePath = 'source.config.enable_cross_platform_lineage';
export const ENABLE_CROSS_PLATFORM_LINEAGE: RecipeField = {
    name: 'enable_cross_platform_lineage',
    label: 'Enable Cross-Platform Lineage',
    tooltip:
        'Whether to extract cross-platform lineage (e.g., connections to BigQuery, etc.). Only applicable if extract_lineage_from_formulas is True.',
    type: FieldType.BOOLEAN,
    fieldPath: enableCrossPlatformLineagePath,
    required: false,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const enableCrossPlatformLineage = get(recipe, enableCrossPlatformLineagePath);
        if (enableCrossPlatformLineage !== undefined && enableCrossPlatformLineage !== null) {
            return enableCrossPlatformLineage;
        }
        return true;
    },
};

const extractColumnLevelLineagePath = 'source.config.extract_column_level_lineage';
export const EXTRACT_COLUMN_LEVEL_LINEAGE: RecipeField = {
    name: 'extract_column_level_lineage',
    label: 'Extract Column-Level Lineage',
    tooltip: 'Whether to extract column-level lineage from Google Sheets formulas.',
    type: FieldType.BOOLEAN,
    fieldPath: extractColumnLevelLineagePath,
    required: false,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const extractColumnLevelLineage = get(recipe, extractColumnLevelLineagePath);
        if (extractColumnLevelLineage !== undefined && extractColumnLevelLineage !== null) {
            return extractColumnLevelLineage;
        }
        return true;
    },
};

const scanSharedDrivesPath = 'source.config.scan_shared_drives';
export const SCAN_SHARED_DRIVES: RecipeField = {
    name: 'scan_shared_drives',
    label: 'Scan Shared Drives',
    tooltip: "Whether to scan sheets in shared drives in addition to 'My Drive'.",
    type: FieldType.BOOLEAN,
    fieldPath: scanSharedDrivesPath,
    required: false,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const scanSharedDrives = get(recipe, scanSharedDrivesPath);
        if (scanSharedDrives !== undefined && scanSharedDrives !== null) {
            return scanSharedDrives;
        }
        return false;
    },
};

const sheetsAsDatasetPath = 'source.config.sheets_as_datasets';
export const SHEETS_AS_DATASETS: RecipeField = {
    name: 'sheets_as_datasets',
    label: 'Treat Sheets as Datasets',
    tooltip: (
        <span>
            <TipSection>
                If enabled, each individual sheet within a Google Sheets document will be treated as a separate dataset.
                The Google Sheets document itself will be represented as a container (similar to a database or schema).
            </TipSection>
            <TipSection>
                If disabled, each Google Sheets document will be treated as a dataset, with individual sheets
                represented as fields within that dataset.
            </TipSection>
            <TipSection>
                Enable this option for more granular metadata and lineage when your individual sheets represent distinct
                logical datasets.
            </TipSection>
        </span>
    ),
    type: FieldType.BOOLEAN,
    fieldPath: sheetsAsDatasetPath,
    required: false,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const sheetsAsDatasets = get(recipe, sheetsAsDatasetPath);
        if (sheetsAsDatasets !== undefined && sheetsAsDatasets !== null) {
            return sheetsAsDatasets;
        }
        return false;
    },
};

const sheetAllowFieldPath = 'source.config.sheet_patterns.allow';
export const SHEET_ALLOW: RecipeField = {
    name: 'sheet_patterns.allow',
    label: 'Allow Patterns',
    tooltip:
        'Only include specific Google Sheets by providing their name, or a Regular Expression (REGEX). If not provided, all sheets will be included.',
    placeholder: 'sheet_name',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: sheetAllowFieldPath,
    rules: null,
    section: 'Sheet Patterns',
};

const sheetDenyFieldPath = 'source.config.sheet_patterns.deny';
export const SHEET_DENY: RecipeField = {
    name: 'sheet_patterns.deny',
    label: 'Deny Patterns',
    tooltip:
        'Exclude specific Google Sheets by providing their name, or a Regular Expression (REGEX). If not provided, all sheets will be included. Deny patterns always take precedence over Allow patterns.',
    placeholder: 'sheet_name',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: sheetDenyFieldPath,
    rules: null,
    section: 'Sheet Patterns',
};

const folderAllowFieldPath = 'source.config.folder_patterns.allow';
export const FOLDER_ALLOW: RecipeField = {
    name: 'folder_patterns.allow',
    label: 'Allow Patterns',
    tooltip:
        'Only include Google Drive folders by providing their name, or a Regular Expression (REGEX). If not provided, all folders will be included.',
    placeholder: 'folder_name',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: folderAllowFieldPath,
    rules: null,
    section: 'Folder Patterns',
};

const folderDenyFieldPath = 'source.config.folder_patterns.deny';
export const FOLDER_DENY: RecipeField = {
    name: 'folder_patterns.deny',
    label: 'Deny Patterns',
    tooltip:
        'Exclude Google Drive folders by providing their name, or a Regular Expression (REGEX). If not provided, all folders will be included. Deny patterns always take precedence over Allow patterns.',
    placeholder: 'folder_name',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: folderDenyFieldPath,
    rules: null,
    section: 'Folder Patterns',
};

const enableProfilingPath = 'source.config.profiling.enabled';
export const ENABLE_PROFILING: RecipeField = {
    name: 'profiling.enabled',
    label: 'Enable Profiling',
    tooltip: 'Whether to enable profiling for Google Sheets.',
    type: FieldType.BOOLEAN,
    fieldPath: enableProfilingPath,
    required: false,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => {
        const enableProfiling = get(recipe, enableProfilingPath);
        if (enableProfiling !== undefined && enableProfiling !== null) {
            return enableProfiling;
        }
        return false;
    },
};

export const MAX_PROFILING_ROWS: RecipeField = {
    name: 'profiling.max_rows',
    label: 'Max Profiling Rows',
    tooltip: 'Maximum number of rows to profile per sheet.',
    type: FieldType.NUMBER,
    fieldPath: 'source.config.profiling.max_rows',
    placeholder: '10000',
    required: false,
    rules: null,
};

export const MAX_NUMBER_OF_FIELDS_TO_PROFILE: RecipeField = {
    name: 'profiling.max_number_of_fields_to_profile',
    label: 'Max Number of Fields to Profile',
    tooltip:
        'A positive integer that specifies the maximum number of columns to profile for any sheet. Leave empty to profile all columns.',
    type: FieldType.NUMBER,
    fieldPath: 'source.config.profiling.max_number_of_fields_to_profile',
    placeholder: 'Leave empty to profile all columns',
    required: false,
    rules: null,
};
