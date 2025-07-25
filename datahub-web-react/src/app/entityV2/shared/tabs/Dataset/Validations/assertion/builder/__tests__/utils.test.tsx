import { describe, expect, it } from 'vitest';

import {
    getDefaultFreshnessSourceOption,
    getFreshnessSourceOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/utils';
import { BIGQUERY_URN, DATABRICKS_URN, REDSHIFT_URN, SNOWFLAKE_URN } from '@app/ingest/source/builder/constants';
import { DBT_URN } from '@app/ingestV2/source/builder/constants';

import { DatasetFreshnessSourceType } from '@types';

describe('getFreshnessSourceOptions', () => {
    describe('when connection does not exist', () => {
        it('should return only DatahubOperation for any platform when connectionForEntityExists is false', () => {
            const platforms = [SNOWFLAKE_URN, BIGQUERY_URN, REDSHIFT_URN, DATABRICKS_URN, 'any-platform'];

            platforms.forEach((platformUrn) => {
                const result = getFreshnessSourceOptions(platformUrn, false);

                expect(result).toHaveLength(1);
                expect(result[0].type).toBe(DatasetFreshnessSourceType.DatahubOperation);
                expect(result[0].name).toBe('DataHub Operation');
            });
        });
    });

    describe('when platform is not eligible for monitoring', () => {
        it('should return only DatahubOperation for DBT platform even when connection exists', () => {
            const result = getFreshnessSourceOptions(DBT_URN, true);

            expect(result).toHaveLength(1);
            expect(result[0].type).toBe(DatasetFreshnessSourceType.DatahubOperation);
            expect(result[0].name).toBe('DataHub Operation');
        });
    });

    describe('when platform is supported and connection exists', () => {
        it('should return Snowflake-specific source options', () => {
            const result = getFreshnessSourceOptions(SNOWFLAKE_URN, true);

            expect(result.length).toBeGreaterThan(1);

            const sourceTypes = result.map((option) => option.type);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.AuditLog);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.InformationSchema);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.FieldValue);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.DatahubOperation);

            // Should not include FileMetadata as it's not supported by Snowflake
            expect(sourceTypes).not.toContain(DatasetFreshnessSourceType.FileMetadata);
        });

        it('should return BigQuery-specific source options', () => {
            const result = getFreshnessSourceOptions(BIGQUERY_URN, true);

            expect(result.length).toBeGreaterThan(1);

            const sourceTypes = result.map((option) => option.type);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.AuditLog);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.InformationSchema);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.FieldValue);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.DatahubOperation);

            // Should not include FileMetadata as it's not supported by BigQuery
            expect(sourceTypes).not.toContain(DatasetFreshnessSourceType.FileMetadata);
        });

        it('should return Redshift-specific source options', () => {
            const result = getFreshnessSourceOptions(REDSHIFT_URN, true);

            expect(result.length).toBeGreaterThan(1);

            const sourceTypes = result.map((option) => option.type);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.AuditLog);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.FieldValue);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.DatahubOperation);

            // Should not include InformationSchema and FileMetadata as they're not supported by Redshift
            expect(sourceTypes).not.toContain(DatasetFreshnessSourceType.InformationSchema);
            expect(sourceTypes).not.toContain(DatasetFreshnessSourceType.FileMetadata);
        });

        it('should return Databricks-specific source options', () => {
            const result = getFreshnessSourceOptions(DATABRICKS_URN, true);

            expect(result.length).toBeGreaterThan(1);

            const sourceTypes = result.map((option) => option.type);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.AuditLog);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.InformationSchema);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.FieldValue);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.FileMetadata);
            expect(sourceTypes).toContain(DatasetFreshnessSourceType.DatahubOperation);
        });
    });

    describe('when platform is not in PLATFORM_ASSERTION_CONFIGS but is eligible', () => {
        it('should return DatahubOperation as fallback for unknown but eligible platforms', () => {
            const unknownPlatformUrn = 'urn:li:dataPlatform:mysql';
            const result = getFreshnessSourceOptions(unknownPlatformUrn, true);

            // Updated: implementation now provides DatahubOperation as fallback
            expect(result).toHaveLength(1);
            expect(result[0].type).toBe(DatasetFreshnessSourceType.DatahubOperation);
        });
    });

    describe('edge cases', () => {
        it('should handle empty platform URN', () => {
            const result = getFreshnessSourceOptions('', true);

            expect(result).toHaveLength(1);
            expect(result[0].type).toBe(DatasetFreshnessSourceType.DatahubOperation);
        });

        it('should handle null platform URN', () => {
            const result = getFreshnessSourceOptions(null as any, true);

            expect(result).toHaveLength(1);
            expect(result[0].type).toBe(DatasetFreshnessSourceType.DatahubOperation);
        });

        it('should handle undefined platform URN', () => {
            const result = getFreshnessSourceOptions(undefined as any, true);

            expect(result).toHaveLength(1);
            expect(result[0].type).toBe(DatasetFreshnessSourceType.DatahubOperation);
        });
    });

    describe('return value structure', () => {
        it('should return options with correct structure', () => {
            const result = getFreshnessSourceOptions(SNOWFLAKE_URN, true);

            result.forEach((option) => {
                expect(option).toHaveProperty('type');
                expect(option).toHaveProperty('name');
                expect(option).toHaveProperty('description');
                expect(option).toHaveProperty('allowedScheduleTypes');
                expect(Array.isArray(option.allowedScheduleTypes)).toBe(true);

                // Type should be a valid DatasetFreshnessSourceType
                expect(Object.values(DatasetFreshnessSourceType)).toContain(option.type);
            });
        });

        it('should return options that include field value options with proper structure', () => {
            const result = getFreshnessSourceOptions(SNOWFLAKE_URN, true);

            const fieldValueOptions = result.filter((option) => option.type === DatasetFreshnessSourceType.FieldValue);

            // Should have both Last Modified Column and High Watermark Column options
            expect(fieldValueOptions.length).toBe(2);

            fieldValueOptions.forEach((option) => {
                expect(option).toHaveProperty('field');
                expect(option.field).toHaveProperty('kind');
                expect(option.field).toHaveProperty('dataTypes');
                expect(option.field?.dataTypes).toBeInstanceOf(Set);
            });
        });
    });
});

describe('getDefaultFreshnessSourceOption', () => {
    describe('when connection does not exist', () => {
        it('should return DatahubOperation for any platform when monitorsConnectionForEntityExists is false', () => {
            const platforms = [SNOWFLAKE_URN, BIGQUERY_URN, REDSHIFT_URN, DATABRICKS_URN, 'any-platform'];

            platforms.forEach((platformUrn) => {
                const result = getDefaultFreshnessSourceOption(platformUrn, false);
                expect(result).toBe(DatasetFreshnessSourceType.DatahubOperation);
            });
        });
    });

    describe('when platform is not eligible for monitoring', () => {
        it('should return DatahubOperation for DBT platform even when connection exists', () => {
            const result = getDefaultFreshnessSourceOption(DBT_URN, true);
            expect(result).toBe(DatasetFreshnessSourceType.DatahubOperation);
        });
    });

    describe('when platform is supported and connection exists', () => {
        it('should return InformationSchema as default for Snowflake', () => {
            const result = getDefaultFreshnessSourceOption(SNOWFLAKE_URN, true);
            expect(result).toBe(DatasetFreshnessSourceType.InformationSchema);
        });

        it('should return InformationSchema as default for BigQuery', () => {
            const result = getDefaultFreshnessSourceOption(BIGQUERY_URN, true);
            expect(result).toBe(DatasetFreshnessSourceType.InformationSchema);
        });

        it('should return AuditLog as default for Redshift', () => {
            const result = getDefaultFreshnessSourceOption(REDSHIFT_URN, true);
            expect(result).toBe(DatasetFreshnessSourceType.AuditLog);
        });

        it('should return AuditLog as default for Databricks', () => {
            const result = getDefaultFreshnessSourceOption(DATABRICKS_URN, true);
            expect(result).toBe(DatasetFreshnessSourceType.AuditLog);
        });
    });

    describe('when platform is not in PLATFORM_ASSERTION_CONFIGS but is eligible', () => {
        it('should return DatahubOperation as fallback for unknown but eligible platforms', () => {
            const unknownPlatformUrn = 'urn:li:dataPlatform:mysql';
            const result = getDefaultFreshnessSourceOption(unknownPlatformUrn, true);
            // Updated: since mysql is not in PLATFORM_ASSERTION_CONFIGS, it's not eligible for direct observe queries
            // so the correct fallback is DatahubOperation, not AuditLog
            expect(result).toBe(DatasetFreshnessSourceType.DatahubOperation);
        });
    });

    describe('edge cases', () => {
        it('should handle empty platform URN when connection exists (empty URN is ineligible)', () => {
            const result = getDefaultFreshnessSourceOption('', true);
            expect(result).toBe(DatasetFreshnessSourceType.DatahubOperation);
        });

        it('should handle null platform URN when connection exists (null URN is ineligible)', () => {
            const result = getDefaultFreshnessSourceOption(null as any, true);
            expect(result).toBe(DatasetFreshnessSourceType.DatahubOperation);
        });

        it('should handle undefined platform URN when connection exists (undefined URN is ineligible)', () => {
            const result = getDefaultFreshnessSourceOption(undefined as any, true);
            expect(result).toBe(DatasetFreshnessSourceType.DatahubOperation);
        });

        it('should handle empty platform URN when connection does not exist', () => {
            const result = getDefaultFreshnessSourceOption('', false);
            expect(result).toBe(DatasetFreshnessSourceType.DatahubOperation);
        });

        it('should handle null platform URN when connection does not exist', () => {
            const result = getDefaultFreshnessSourceOption(null as any, false);
            expect(result).toBe(DatasetFreshnessSourceType.DatahubOperation);
        });

        it('should handle undefined platform URN when connection does not exist', () => {
            const result = getDefaultFreshnessSourceOption(undefined as any, false);
            expect(result).toBe(DatasetFreshnessSourceType.DatahubOperation);
        });
    });

    describe('parameter combinations', () => {
        it('should prioritize connection state over platform eligibility', () => {
            // Even for eligible platforms, if no connection exists, should return DatahubOperation
            const eligiblePlatforms = [SNOWFLAKE_URN, BIGQUERY_URN, REDSHIFT_URN, DATABRICKS_URN];

            eligiblePlatforms.forEach((platformUrn) => {
                const result = getDefaultFreshnessSourceOption(platformUrn, false);
                expect(result).toBe(DatasetFreshnessSourceType.DatahubOperation);
            });
        });

        it('should return DatahubOperation for ineligible platforms regardless of connection state', () => {
            // DBT is ineligible, so should return DatahubOperation regardless of connection
            expect(getDefaultFreshnessSourceOption(DBT_URN, true)).toBe(DatasetFreshnessSourceType.DatahubOperation);
            expect(getDefaultFreshnessSourceOption(DBT_URN, false)).toBe(DatasetFreshnessSourceType.DatahubOperation);
        });
    });
});
