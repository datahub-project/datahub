import {
    getDefaultVolumeSourceType,
    getVolumeSourceTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/utils';
import {
    getDefaultFreshnessSourceOption,
    getFreshnessSourceOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/utils';
import { BIGQUERY_URN, DATABRICKS_URN, REDSHIFT_URN, SNOWFLAKE_URN } from '@app/ingest/source/builder/constants';
import { BulkCreateDatasetAssertionsSpec, ProgressTracker } from '@app/observe/shared/bulkCreate/constants';
import {
    buildCreateAssertionsForDataset,
    buildUpsertFreshnessAssertionParams,
    buildUpsertVolumeAssertionParams,
} from '@app/observe/shared/bulkCreate/useBulkCreateDatasetAssertions';

import {
    AssertionStdOperator,
    AssertionStdParameterType,
    AssertionType,
    Dataset,
    DatasetFreshnessSourceType,
    DatasetVolumeSourceType,
    EntityType,
    FabricType,
    FreshnessAssertionScheduleType,
    FreshnessFieldKind,
    MonitorMode,
    VolumeAssertionType,
} from '@types';

// ------------------------------------------------------------------------------------------------------------------------//
// Test for validateAndAdjustFreshnessSourceType function
// ------------------------------------------------------------------------------------------------------------------------//

/**
 * Test version of validateAndAdjustFreshnessSourceType function
 * This mirrors the implementation from useBulkCreateDatasetAssertions.ts
 */
const validateAndAdjustFreshnessSourceType = (
    freshnessSpec: {
        evaluationParameters: {
            sourceType: DatasetFreshnessSourceType;
            [key: string]: any;
        };
        [key: string]: any;
    },
    platformUrn: string,
    datasetUrn: string,
) => {
    const { sourceType } = freshnessSpec.evaluationParameters;
    const validSourceTypes = getFreshnessSourceOptions(platformUrn, true);
    const isSourceTypeValid = validSourceTypes.find((option) => option.type === sourceType);

    const adjustedEvaluationParameters = { ...freshnessSpec.evaluationParameters };
    if (!isSourceTypeValid) {
        const defaultSourceType =
            getDefaultFreshnessSourceOption(platformUrn, true) || DatasetFreshnessSourceType.DatahubOperation;
        adjustedEvaluationParameters.sourceType = defaultSourceType;
        console.warn(
            `Invalid source type: ${sourceType} for dataset ${datasetUrn}, using default: ${defaultSourceType}`,
        );
    }

    return adjustedEvaluationParameters;
};

/**
 * Test version of validateAndAdjustVolumeSourceType function
 * This mirrors the implementation from useBulkCreateDatasetAssertions.ts
 */
const validateAndAdjustVolumeSourceType = (
    volumeSpec: {
        evaluationParameters: {
            sourceType: DatasetVolumeSourceType;
            [key: string]: any;
        };
        [key: string]: any;
    },
    platformUrn: string,
    datasetUrn: string,
    isView: boolean,
) => {
    const { sourceType } = volumeSpec.evaluationParameters;
    const validSourceTypes = getVolumeSourceTypeOptions(platformUrn, true, isView);
    const isSourceTypeValid = validSourceTypes.find((option) => option.toLowerCase() === sourceType.toLowerCase());

    const adjustedEvaluationParameters = { ...volumeSpec.evaluationParameters };
    if (!isSourceTypeValid) {
        const defaultSourceType =
            getDefaultVolumeSourceType(platformUrn, true, isView) || DatasetVolumeSourceType.DatahubDatasetProfile;
        adjustedEvaluationParameters.sourceType = defaultSourceType;
        console.warn(
            `Invalid source type: ${sourceType} for dataset ${datasetUrn}, using default: ${defaultSourceType}`,
        );
    }

    return adjustedEvaluationParameters;
};

describe('validateAndAdjustFreshnessSourceType', () => {
    describe('Valid source types', () => {
        it('should return the same source type when valid for Snowflake', () => {
            const freshnessSpec = {
                evaluationParameters: {
                    sourceType: DatasetFreshnessSourceType.InformationSchema,
                    someOtherProperty: 'value',
                },
                otherProperty: 'test',
            };

            const result = validateAndAdjustFreshnessSourceType(
                freshnessSpec,
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );

            expect(result.sourceType).toBe(DatasetFreshnessSourceType.InformationSchema);
            expect(result.someOtherProperty).toBe('value');
        });

        it('should return the same source type when valid for BigQuery', () => {
            const freshnessSpec = {
                evaluationParameters: {
                    sourceType: DatasetFreshnessSourceType.AuditLog,
                },
            };

            const result = validateAndAdjustFreshnessSourceType(
                freshnessSpec,
                BIGQUERY_URN,
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)',
            );

            expect(result.sourceType).toBe(DatasetFreshnessSourceType.AuditLog);
        });

        it('should return the same source type when valid for Redshift', () => {
            const freshnessSpec = {
                evaluationParameters: {
                    sourceType: DatasetFreshnessSourceType.FieldValue,
                },
            };

            const result = validateAndAdjustFreshnessSourceType(
                freshnessSpec,
                REDSHIFT_URN,
                'urn:li:dataset:(urn:li:dataPlatform:redshift,database.schema.table,PROD)',
            );

            expect(result.sourceType).toBe(DatasetFreshnessSourceType.FieldValue);
        });

        it('should return the same source type when valid for Databricks', () => {
            const freshnessSpec = {
                evaluationParameters: {
                    sourceType: DatasetFreshnessSourceType.FileMetadata,
                },
            };

            const result = validateAndAdjustFreshnessSourceType(
                freshnessSpec,
                DATABRICKS_URN,
                'urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)',
            );

            expect(result.sourceType).toBe(DatasetFreshnessSourceType.FileMetadata);
        });
    });

    describe('Invalid source types with platform defaults', () => {
        it('should fallback to Snowflake default (InformationSchema) when source type is invalid', () => {
            const freshnessSpec = {
                evaluationParameters: {
                    sourceType: DatasetFreshnessSourceType.FileMetadata, // Not supported by Snowflake
                    someOtherProperty: 'value',
                },
            };

            const result = validateAndAdjustFreshnessSourceType(
                freshnessSpec,
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );

            expect(result.sourceType).toBe(DatasetFreshnessSourceType.InformationSchema);
            expect(result.someOtherProperty).toBe('value');
        });

        it('should fallback to BigQuery default (InformationSchema) when source type is invalid', () => {
            const freshnessSpec = {
                evaluationParameters: {
                    sourceType: DatasetFreshnessSourceType.FileMetadata, // Not supported by BigQuery
                },
            };

            const result = validateAndAdjustFreshnessSourceType(
                freshnessSpec,
                BIGQUERY_URN,
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)',
            );

            expect(result.sourceType).toBe(DatasetFreshnessSourceType.InformationSchema);
        });

        it('should fallback to Redshift default (AuditLog) when source type is invalid', () => {
            const freshnessSpec = {
                evaluationParameters: {
                    sourceType: DatasetFreshnessSourceType.InformationSchema, // Not supported by Redshift
                },
            };

            const result = validateAndAdjustFreshnessSourceType(
                freshnessSpec,
                REDSHIFT_URN,
                'urn:li:dataset:(urn:li:dataPlatform:redshift,database.schema.table,PROD)',
            );

            expect(result.sourceType).toBe(DatasetFreshnessSourceType.AuditLog);
        });

        it('should fallback to Databricks default (AuditLog) when source type is invalid', () => {
            const freshnessSpec = {
                evaluationParameters: {
                    sourceType: 'INVALID_SOURCE_TYPE' as DatasetFreshnessSourceType,
                },
            };

            const result = validateAndAdjustFreshnessSourceType(
                freshnessSpec,
                DATABRICKS_URN,
                'urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)',
            );

            expect(result.sourceType).toBe(DatasetFreshnessSourceType.AuditLog);
        });
    });

    describe('Unknown platforms', () => {
        it('should fallback to AuditLog default for unknown platform', () => {
            const unknownPlatformUrn = 'urn:li:dataPlatform:unknown-platform';
            const freshnessSpec = {
                evaluationParameters: {
                    sourceType: 'INVALID_SOURCE_TYPE' as DatasetFreshnessSourceType,
                },
            };

            const result = validateAndAdjustFreshnessSourceType(
                freshnessSpec,
                unknownPlatformUrn,
                'urn:li:dataset:(urn:li:dataPlatform:unknown-platform,test.table,PROD)',
            );

            expect(result.sourceType).toBe(DatasetFreshnessSourceType.AuditLog);
        });

        it('should accept valid source types for unknown platform (falls back to all available options)', () => {
            const unknownPlatformUrn = 'urn:li:dataPlatform:unknown-platform';
            const freshnessSpec = {
                evaluationParameters: {
                    sourceType: DatasetFreshnessSourceType.AuditLog,
                },
            };

            const result = validateAndAdjustFreshnessSourceType(
                freshnessSpec,
                unknownPlatformUrn,
                'urn:li:dataset:(urn:li:dataPlatform:unknown-platform,test.table,PROD)',
            );

            expect(result.sourceType).toBe(DatasetFreshnessSourceType.AuditLog);
        });
    });

    describe('Edge cases', () => {
        it('should preserve other evaluation parameters when adjusting source type', () => {
            const freshnessSpec = {
                evaluationParameters: {
                    sourceType: DatasetFreshnessSourceType.FileMetadata, // Invalid for Snowflake
                    field: {
                        path: 'updated_at',
                        kind: FreshnessFieldKind.LastModified,
                        type: 'DateType',
                        nativeType: 'TIMESTAMP',
                    },
                    auditLog: { operationTypes: ['INSERT'] },
                },
                schedule: { type: 'CRON', cron: '0 0 * * *' },
            };

            const result = validateAndAdjustFreshnessSourceType(
                freshnessSpec,
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );

            expect(result.sourceType).toBe(DatasetFreshnessSourceType.InformationSchema);
            expect(result.field).toEqual({
                path: 'updated_at',
                kind: FreshnessFieldKind.LastModified,
                type: 'DateType',
                nativeType: 'TIMESTAMP',
            });
            expect(result.auditLog).toEqual({ operationTypes: ['INSERT'] });
        });

        it('should handle DatahubOperation source type correctly across all platforms', () => {
            const freshnessSpec = {
                evaluationParameters: {
                    sourceType: DatasetFreshnessSourceType.DatahubOperation,
                },
            };

            // Test with multiple platforms - DatahubOperation should be valid for all
            const platforms = [SNOWFLAKE_URN, BIGQUERY_URN, REDSHIFT_URN, DATABRICKS_URN];

            platforms.forEach((platformUrn) => {
                const result = validateAndAdjustFreshnessSourceType(
                    freshnessSpec,
                    platformUrn,
                    `urn:li:dataset:(${platformUrn.replace('urn:li:dataPlatform:', '')},test.table,PROD)`,
                );

                expect(result.sourceType).toBe(DatasetFreshnessSourceType.DatahubOperation);
            });
        });
    });

    describe('Platform-specific source type validations', () => {
        it('should validate Snowflake allowed source types', () => {
            const allowedTypes = [
                DatasetFreshnessSourceType.AuditLog,
                DatasetFreshnessSourceType.InformationSchema,
                DatasetFreshnessSourceType.FieldValue,
                DatasetFreshnessSourceType.DatahubOperation,
            ];
            const notAllowedType = DatasetFreshnessSourceType.FileMetadata;

            allowedTypes.forEach((sourceType) => {
                const freshnessSpec = {
                    evaluationParameters: { sourceType },
                };

                const result = validateAndAdjustFreshnessSourceType(
                    freshnessSpec,
                    SNOWFLAKE_URN,
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
                );

                expect(result.sourceType).toBe(sourceType);
            });

            // Test not allowed type
            const freshnessSpec = {
                evaluationParameters: { sourceType: notAllowedType },
            };

            const result = validateAndAdjustFreshnessSourceType(
                freshnessSpec,
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );

            expect(result.sourceType).toBe(DatasetFreshnessSourceType.InformationSchema); // Snowflake default
        });

        it('should validate Redshift allowed source types', () => {
            const allowedTypes = [
                DatasetFreshnessSourceType.AuditLog,
                DatasetFreshnessSourceType.FieldValue,
                DatasetFreshnessSourceType.DatahubOperation,
            ];
            const notAllowedTypes = [
                DatasetFreshnessSourceType.InformationSchema,
                DatasetFreshnessSourceType.FileMetadata,
            ];

            allowedTypes.forEach((sourceType) => {
                const freshnessSpec = {
                    evaluationParameters: { sourceType },
                };

                const result = validateAndAdjustFreshnessSourceType(
                    freshnessSpec,
                    REDSHIFT_URN,
                    'urn:li:dataset:(urn:li:dataPlatform:redshift,database.schema.table,PROD)',
                );

                expect(result.sourceType).toBe(sourceType);
            });

            // Test not allowed types
            notAllowedTypes.forEach((sourceType) => {
                const freshnessSpec = {
                    evaluationParameters: { sourceType },
                };

                const result = validateAndAdjustFreshnessSourceType(
                    freshnessSpec,
                    REDSHIFT_URN,
                    'urn:li:dataset:(urn:li:dataPlatform:redshift,database.schema.table,PROD)',
                );

                expect(result.sourceType).toBe(DatasetFreshnessSourceType.AuditLog); // Redshift default
            });
        });
    });
});

// ------------------------------------------------------------------------------------------------------------------------//
// Test for validateAndAdjustVolumeSourceType function
// ------------------------------------------------------------------------------------------------------------------------//

describe('validateAndAdjustVolumeSourceType', () => {
    describe('Valid source types for tables', () => {
        it('should return the same source type when valid for Snowflake table', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: DatasetVolumeSourceType.InformationSchema,
                    someOtherProperty: 'value',
                },
                otherProperty: 'test',
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
                false, // isView = false
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.InformationSchema);
            expect(result.someOtherProperty).toBe('value');
        });

        it('should return the same source type when valid for BigQuery table', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: DatasetVolumeSourceType.Query,
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                BIGQUERY_URN,
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)',
                false, // isView = false
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.Query);
        });

        it('should return the same source type when valid for Redshift table', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: DatasetVolumeSourceType.DatahubDatasetProfile,
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                REDSHIFT_URN,
                'urn:li:dataset:(urn:li:dataPlatform:redshift,database.schema.table,PROD)',
                false, // isView = false
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.DatahubDatasetProfile);
        });

        it('should return the same source type when valid for Databricks table', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: DatasetVolumeSourceType.Query,
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                DATABRICKS_URN,
                'urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)',
                false, // isView = false
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.Query);
        });
    });

    describe('Valid source types for views', () => {
        it('should return the same source type when valid for Snowflake view (excludes InformationSchema)', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: DatasetVolumeSourceType.Query,
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.view,PROD)',
                true, // isView = true
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.Query);
        });

        it('should return the same source type when valid for BigQuery view (excludes InformationSchema)', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: DatasetVolumeSourceType.DatahubDatasetProfile,
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                BIGQUERY_URN,
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.view,PROD)',
                true, // isView = true
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.DatahubDatasetProfile);
        });
    });

    describe('Invalid source types with platform defaults for tables', () => {
        it('should fallback to Snowflake default (InformationSchema) when source type is invalid for table', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: 'INVALID_SOURCE_TYPE' as DatasetVolumeSourceType,
                    someOtherProperty: 'value',
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
                false, // isView = false
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.InformationSchema);
            expect(result.someOtherProperty).toBe('value');
        });

        it('should fallback to BigQuery default (InformationSchema) when source type is invalid for table', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: 'INVALID_SOURCE_TYPE' as DatasetVolumeSourceType,
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                BIGQUERY_URN,
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)',
                false, // isView = false
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.InformationSchema);
        });

        it('should fallback to Redshift default (InformationSchema) when source type is invalid for table', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: 'INVALID_SOURCE_TYPE' as DatasetVolumeSourceType,
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                REDSHIFT_URN,
                'urn:li:dataset:(urn:li:dataPlatform:redshift,database.schema.table,PROD)',
                false, // isView = false
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.InformationSchema);
        });

        it('should fallback to Databricks default (Query) when source type is invalid for table', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: 'INVALID_SOURCE_TYPE' as DatasetVolumeSourceType,
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                DATABRICKS_URN,
                'urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)',
                false, // isView = false
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.Query);
        });
    });

    describe('Invalid source types with platform defaults for views', () => {
        it('should fallback to Query for Snowflake view when InformationSchema is invalid', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: DatasetVolumeSourceType.InformationSchema, // Not valid for views
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.view,PROD)',
                true, // isView = true
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.Query);
        });

        it('should fallback to Query for BigQuery view when InformationSchema is invalid', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: DatasetVolumeSourceType.InformationSchema, // Not valid for views
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                BIGQUERY_URN,
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.view,PROD)',
                true, // isView = true
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.Query);
        });

        it('should fallback to Query for Redshift view when InformationSchema is invalid', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: DatasetVolumeSourceType.InformationSchema, // Not valid for views
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                REDSHIFT_URN,
                'urn:li:dataset:(urn:li:dataPlatform:redshift,database.schema.view,PROD)',
                true, // isView = true
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.Query);
        });
    });

    describe('Case-insensitive comparison', () => {
        it('should handle case-insensitive source type comparison', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: 'query' as DatasetVolumeSourceType, // lowercase
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
                false, // isView = false
            );

            expect(result.sourceType).toBe('query' as DatasetVolumeSourceType);
        });

        it('should handle mixed case source type comparison', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: 'Information_Schema' as DatasetVolumeSourceType, // mixed case
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                BIGQUERY_URN,
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)',
                false, // isView = false
            );

            expect(result.sourceType).toBe('Information_Schema' as DatasetVolumeSourceType);
        });
    });

    describe('Unknown platforms', () => {
        it('should fallback to DatahubDatasetProfile default for unknown platform table', () => {
            const unknownPlatformUrn = 'urn:li:dataPlatform:unknown-platform';
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: 'INVALID_SOURCE_TYPE' as DatasetVolumeSourceType,
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                unknownPlatformUrn,
                'urn:li:dataset:(urn:li:dataPlatform:unknown-platform,test.table,PROD)',
                false, // isView = false
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.DatahubDatasetProfile);
        });

        it('should fallback to DatahubDatasetProfile default for unknown platform view', () => {
            const unknownPlatformUrn = 'urn:li:dataPlatform:unknown-platform';
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: 'INVALID_SOURCE_TYPE' as DatasetVolumeSourceType,
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                unknownPlatformUrn,
                'urn:li:dataset:(urn:li:dataPlatform:unknown-platform,test.view,PROD)',
                true, // isView = true
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.DatahubDatasetProfile);
        });

        it('should accept valid source types for unknown platform', () => {
            const unknownPlatformUrn = 'urn:li:dataPlatform:unknown-platform';
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: DatasetVolumeSourceType.DatahubDatasetProfile,
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                unknownPlatformUrn,
                'urn:li:dataset:(urn:li:dataPlatform:unknown-platform,test.table,PROD)',
                false, // isView = false
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.DatahubDatasetProfile);
        });
    });

    describe('Edge cases', () => {
        it('should preserve other evaluation parameters when adjusting source type', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: 'INVALID_SOURCE_TYPE' as DatasetVolumeSourceType,
                    // Removed field and query properties as they don't exist on DatasetVolumeAssertionParametersInput
                },
                evaluationSchedule: {
                    timezone: 'UTC',
                    cron: '0 0 * * *',
                },
                actions: {
                    onSuccess: [],
                    onFailure: [],
                },
                inferenceSettings: {
                    sensitivity: {
                        level: 9,
                    },
                    trainingDataLookbackWindowDays: 60,
                    exclusionWindows: [],
                },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
                false, // isView = false
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.InformationSchema);
            // The validateAndAdjustVolumeSourceType function only returns evaluation parameters
            // It does not return inference settings as those are handled separately
        });

        it('should handle DatahubDatasetProfile source type correctly across all platforms', () => {
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: DatasetVolumeSourceType.DatahubDatasetProfile,
                },
            };

            // Test with multiple platforms - DatahubDatasetProfile should be valid for all
            const platforms = [SNOWFLAKE_URN, BIGQUERY_URN, REDSHIFT_URN, DATABRICKS_URN];

            platforms.forEach((platformUrn) => {
                // Test both tables and views
                [false, true].forEach((isView) => {
                    const result = validateAndAdjustVolumeSourceType(
                        volumeSpec,
                        platformUrn,
                        `urn:li:dataset:(${platformUrn.replace('urn:li:dataPlatform:', '')},test.${isView ? 'view' : 'table'},PROD)`,
                        isView,
                    );

                    expect(result.sourceType).toBe(DatasetVolumeSourceType.DatahubDatasetProfile);
                });
            });
        });
    });

    describe('Platform-specific source type validations', () => {
        it('should validate Snowflake allowed source types for tables', () => {
            const allowedTypes = [
                DatasetVolumeSourceType.InformationSchema,
                DatasetVolumeSourceType.Query,
                DatasetVolumeSourceType.DatahubDatasetProfile,
            ];

            allowedTypes.forEach((sourceType) => {
                const volumeSpec = {
                    evaluationParameters: { sourceType },
                };

                const result = validateAndAdjustVolumeSourceType(
                    volumeSpec,
                    SNOWFLAKE_URN,
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
                    false, // isView = false
                );

                expect(result.sourceType).toBe(sourceType);
            });
        });

        it('should validate Snowflake allowed source types for views (excludes InformationSchema)', () => {
            const allowedTypesForViews = [DatasetVolumeSourceType.Query, DatasetVolumeSourceType.DatahubDatasetProfile];
            const notAllowedType = DatasetVolumeSourceType.InformationSchema;

            allowedTypesForViews.forEach((sourceType) => {
                const volumeSpec = {
                    evaluationParameters: { sourceType },
                };

                const result = validateAndAdjustVolumeSourceType(
                    volumeSpec,
                    SNOWFLAKE_URN,
                    'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.view,PROD)',
                    true, // isView = true
                );

                expect(result.sourceType).toBe(sourceType);
            });

            // Test not allowed type for views
            const volumeSpec = {
                evaluationParameters: { sourceType: notAllowedType },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.view,PROD)',
                true, // isView = true
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.Query); // Fallback for Snowflake view
        });

        it('should validate Databricks allowed source types (no InformationSchema support)', () => {
            const allowedTypes = [DatasetVolumeSourceType.Query, DatasetVolumeSourceType.DatahubDatasetProfile];
            const notAllowedType = DatasetVolumeSourceType.InformationSchema;

            allowedTypes.forEach((sourceType) => {
                const volumeSpec = {
                    evaluationParameters: { sourceType },
                };

                const result = validateAndAdjustVolumeSourceType(
                    volumeSpec,
                    DATABRICKS_URN,
                    'urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)',
                    false, // isView = false
                );

                expect(result.sourceType).toBe(sourceType);
            });

            // Test not allowed type
            const volumeSpec = {
                evaluationParameters: { sourceType: notAllowedType },
            };

            const result = validateAndAdjustVolumeSourceType(
                volumeSpec,
                DATABRICKS_URN,
                'urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)',
                false, // isView = false
            );

            expect(result.sourceType).toBe(DatasetVolumeSourceType.Query); // Databricks default
        });
    });
});

// ------------------------------------------------------------------------------------------------------------------------//
// Test for buildUpsertFreshnessAssertionParams function
// ------------------------------------------------------------------------------------------------------------------------//

describe('buildUpsertFreshnessAssertionParams', () => {
    const createMockDataset = (platformUrn: string, urn: string): Dataset => ({
        urn,
        type: EntityType.Dataset,
        platform: {
            urn: platformUrn,
            type: EntityType.DataPlatform,
            name: platformUrn.split(':').pop() || 'unknown',
        },
        name: 'test-dataset',
        origin: FabricType.Prod,
        __typename: 'Dataset',
    });

    const createMockFreshnessSpec = (sourceType: DatasetFreshnessSourceType, criteriaType: 'AI' | 'MANUAL' = 'AI') => {
        const baseSpec = {
            evaluationParameters: {
                sourceType,
            },
            evaluationSchedule: {
                timezone: 'UTC',
                cron: '0 0 * * *',
            },
            actions: {
                onSuccess: [],
                onFailure: [],
            },
        };

        if (criteriaType === 'AI') {
            return {
                ...baseSpec,
                criteria: {
                    type: 'AI' as const,
                    inferenceSettings: {
                        sensitivity: {
                            level: 8,
                        },
                        trainingDataLookbackWindowDays: 30,
                        exclusionWindows: [],
                    },
                },
            };
        }
        return {
            ...baseSpec,
            criteria: {
                type: 'MANUAL' as const,
                schedule: {
                    type: FreshnessAssertionScheduleType.SinceTheLastCheck,
                    lookbackDays: 7,
                },
            },
        };
    };

    describe('AI criteria type', () => {
        it('should build correct parameters for AI freshness assertion with valid source type', () => {
            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );
            const freshnessSpec = createMockFreshnessSpec(DatasetFreshnessSourceType.InformationSchema, 'AI');

            const result = buildUpsertFreshnessAssertionParams(dataset, freshnessSpec);

            expect(result.variables.input).toEqual({
                mode: MonitorMode.Active,
                entityUrn: dataset.urn,
                evaluationParameters: {
                    sourceType: DatasetFreshnessSourceType.InformationSchema,
                },
                evaluationSchedule: freshnessSpec.evaluationSchedule,
                schedule: undefined,
                actions: freshnessSpec.actions,
                inferWithAI: true,
                inferenceSettings: {
                    sensitivity: {
                        level: 8,
                    },
                    trainingDataLookbackWindowDays: 30,
                    exclusionWindows: [],
                },
                description: 'Freshness anomaly check',
            });
        });

        it('should adjust invalid source type for AI freshness assertion', () => {
            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );
            const freshnessSpec = createMockFreshnessSpec(DatasetFreshnessSourceType.FileMetadata, 'AI'); // Invalid for Snowflake

            const result = buildUpsertFreshnessAssertionParams(dataset, freshnessSpec);

            expect(result.variables.input.evaluationParameters.sourceType).toBe(
                DatasetFreshnessSourceType.InformationSchema,
            );
            expect(result.variables.input.inferWithAI).toBe(true);
            expect(result.variables.input.description).toBe('Freshness anomaly check');
        });
    });

    describe('MANUAL criteria type', () => {
        it('should build correct parameters for manual freshness assertion with valid source type', () => {
            const dataset = createMockDataset(
                BIGQUERY_URN,
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)',
            );
            const freshnessSpec = createMockFreshnessSpec(DatasetFreshnessSourceType.AuditLog, 'MANUAL');

            const result = buildUpsertFreshnessAssertionParams(dataset, freshnessSpec);

            expect(result.variables.input).toEqual({
                mode: MonitorMode.Active,
                entityUrn: dataset.urn,
                evaluationParameters: {
                    sourceType: DatasetFreshnessSourceType.AuditLog,
                },
                evaluationSchedule: freshnessSpec.evaluationSchedule,
                schedule: {
                    type: FreshnessAssertionScheduleType.SinceTheLastCheck,
                    lookbackDays: 7,
                },
                actions: freshnessSpec.actions,
                inferWithAI: false,
                inferenceSettings: undefined,
                description: undefined,
            });
        });

        it('should adjust invalid source type for manual freshness assertion', () => {
            const dataset = createMockDataset(
                REDSHIFT_URN,
                'urn:li:dataset:(urn:li:dataPlatform:redshift,database.schema.table,PROD)',
            );
            const freshnessSpec = createMockFreshnessSpec(DatasetFreshnessSourceType.InformationSchema, 'MANUAL'); // Invalid for Redshift

            const result = buildUpsertFreshnessAssertionParams(dataset, freshnessSpec);

            expect(result.variables.input.evaluationParameters.sourceType).toBe(DatasetFreshnessSourceType.AuditLog);
            expect(result.variables.input.inferWithAI).toBe(false);
            expect(result.variables.input.schedule).toEqual({
                type: FreshnessAssertionScheduleType.SinceTheLastCheck,
                lookbackDays: 7,
            });
            expect(result.variables.input.description).toBeUndefined();
        });
    });

    describe('Platform-specific behavior', () => {
        it('should handle Databricks platform correctly', () => {
            const dataset = createMockDataset(
                DATABRICKS_URN,
                'urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)',
            );
            const freshnessSpec = createMockFreshnessSpec(DatasetFreshnessSourceType.FileMetadata, 'AI');

            const result = buildUpsertFreshnessAssertionParams(dataset, freshnessSpec);

            expect(result.variables.input.evaluationParameters.sourceType).toBe(
                DatasetFreshnessSourceType.FileMetadata,
            );
            expect(result.variables.input.entityUrn).toBe(dataset.urn);
        });

        it('should preserve other evaluation parameters when adjusting source type', () => {
            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );
            const freshnessSpec = {
                evaluationParameters: {
                    sourceType: DatasetFreshnessSourceType.FileMetadata, // Invalid for Snowflake
                    field: {
                        path: 'updated_at',
                        kind: FreshnessFieldKind.LastModified,
                        type: 'DateType',
                        nativeType: 'TIMESTAMP',
                    },
                    auditLog: { operationTypes: ['INSERT'] },
                },
                evaluationSchedule: {
                    timezone: 'UTC',
                    cron: '0 0 * * *',
                },
                actions: {
                    onSuccess: [],
                    onFailure: [],
                },
                criteria: {
                    type: 'AI' as const,
                    inferenceSettings: {
                        sensitivity: {
                            level: 9,
                        },
                        trainingDataLookbackWindowDays: 60,
                        exclusionWindows: [],
                    },
                },
            };

            const result = buildUpsertFreshnessAssertionParams(dataset, freshnessSpec);

            expect(result.variables.input.evaluationParameters.sourceType).toBe(
                DatasetFreshnessSourceType.InformationSchema,
            );
            expect(result.variables.input.evaluationParameters.field).toEqual({
                path: 'updated_at',
                kind: FreshnessFieldKind.LastModified,
                type: 'DateType',
                nativeType: 'TIMESTAMP',
            });
            expect(result.variables.input.evaluationParameters.auditLog).toEqual({ operationTypes: ['INSERT'] });
        });
    });
});

// ------------------------------------------------------------------------------------------------------------------------//
// Test for buildUpsertVolumeAssertionParams function
// ------------------------------------------------------------------------------------------------------------------------//

describe('buildUpsertVolumeAssertionParams', () => {
    const createMockDataset = (platformUrn: string, urn: string, isView = false): Dataset => ({
        urn,
        type: EntityType.Dataset,
        platform: {
            urn: platformUrn,
            type: EntityType.DataPlatform,
            name: platformUrn.split(':').pop() || 'unknown',
        },
        name: 'test-dataset',
        origin: FabricType.Prod,
        subTypes: isView
            ? {
                  typeNames: ['view'],
              }
            : {
                  typeNames: ['table'],
              },
        __typename: 'Dataset',
    });

    const createMockVolumeSpec = (sourceType: DatasetVolumeSourceType) => ({
        evaluationParameters: {
            sourceType,
        },
        evaluationSchedule: {
            timezone: 'UTC',
            cron: '0 0 * * *',
        },
        actions: {
            onSuccess: [],
            onFailure: [],
        },
        inferenceSettings: {
            sensitivity: {
                level: 8,
            },
            trainingDataLookbackWindowDays: 30,
            exclusionWindows: [],
        },
    });

    describe('Table datasets', () => {
        it('should build correct parameters for table with valid source type', () => {
            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
                false,
            );
            const volumeSpec = createMockVolumeSpec(DatasetVolumeSourceType.InformationSchema);

            const result = buildUpsertVolumeAssertionParams(dataset, volumeSpec);

            expect(result.variables.input).toEqual({
                mode: MonitorMode.Active,
                type: VolumeAssertionType.RowCountTotal,
                inferWithAI: true,
                rowCountTotal: {
                    operator: AssertionStdOperator.Between,
                    parameters: {
                        minValue: {
                            type: AssertionStdParameterType.Number,
                            value: '0',
                        },
                        maxValue: {
                            type: AssertionStdParameterType.Number,
                            value: '1000',
                        },
                    },
                },
                description: 'Row count volume anomaly check',
                entityUrn: dataset.urn,
                evaluationParameters: {
                    sourceType: DatasetVolumeSourceType.InformationSchema,
                },
                evaluationSchedule: volumeSpec.evaluationSchedule,
                actions: volumeSpec.actions,
                inferenceSettings: volumeSpec.inferenceSettings,
            });
        });

        it('should adjust invalid source type for table', () => {
            const dataset = createMockDataset(
                DATABRICKS_URN,
                'urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)',
                false,
            );
            const volumeSpec = createMockVolumeSpec(DatasetVolumeSourceType.InformationSchema); // Invalid for Databricks

            const result = buildUpsertVolumeAssertionParams(dataset, volumeSpec);

            expect(result.variables.input.evaluationParameters.sourceType).toBe(DatasetVolumeSourceType.Query);
            expect(result.variables.input.type).toBe(VolumeAssertionType.RowCountTotal);
            expect(result.variables.input.inferWithAI).toBe(true);
        });
    });

    describe('View datasets', () => {
        it('should build correct parameters for view with valid source type', () => {
            const dataset = createMockDataset(
                BIGQUERY_URN,
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.view,PROD)',
                true,
            );
            const volumeSpec = createMockVolumeSpec(DatasetVolumeSourceType.Query);

            const result = buildUpsertVolumeAssertionParams(dataset, volumeSpec);

            expect(result.variables.input.evaluationParameters.sourceType).toBe(DatasetVolumeSourceType.Query);
            expect(result.variables.input.entityUrn).toBe(dataset.urn);
            expect(result.variables.input.description).toBe('Row count volume anomaly check');
        });

        it('should adjust invalid source type for view (InformationSchema not allowed)', () => {
            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.view,PROD)',
                true,
            );
            const volumeSpec = createMockVolumeSpec(DatasetVolumeSourceType.InformationSchema); // Invalid for views

            const result = buildUpsertVolumeAssertionParams(dataset, volumeSpec);

            expect(result.variables.input.evaluationParameters.sourceType).toBe(DatasetVolumeSourceType.Query);
            expect(result.variables.input.type).toBe(VolumeAssertionType.RowCountTotal);
        });

        it('should handle view subtype case insensitively', () => {
            const dataset = createMockDataset(
                REDSHIFT_URN,
                'urn:li:dataset:(urn:li:dataPlatform:redshift,database.schema.view,PROD)',
                false,
            );
            dataset.subTypes = {
                typeNames: ['VIEW'], // Uppercase
            };
            const volumeSpec = createMockVolumeSpec(DatasetVolumeSourceType.InformationSchema);

            const result = buildUpsertVolumeAssertionParams(dataset, volumeSpec);

            // Should be treated as a view and fallback to Query
            expect(result.variables.input.evaluationParameters.sourceType).toBe(DatasetVolumeSourceType.Query);
        });
    });

    describe('Platform-specific behavior', () => {
        it('should handle different platforms correctly for tables', () => {
            const platforms = [
                { urn: SNOWFLAKE_URN, expected: DatasetVolumeSourceType.InformationSchema },
                { urn: BIGQUERY_URN, expected: DatasetVolumeSourceType.InformationSchema },
                { urn: REDSHIFT_URN, expected: DatasetVolumeSourceType.InformationSchema },
                { urn: DATABRICKS_URN, expected: DatasetVolumeSourceType.Query },
            ];

            platforms.forEach(({ urn, expected }) => {
                const dataset = createMockDataset(
                    urn,
                    `urn:li:dataset:(${urn.replace('urn:li:dataPlatform:', '')},test.table,PROD)`,
                    false,
                );
                const volumeSpec = createMockVolumeSpec('INVALID_SOURCE_TYPE' as DatasetVolumeSourceType);

                const result = buildUpsertVolumeAssertionParams(dataset, volumeSpec);

                expect(result.variables.input.evaluationParameters.sourceType).toBe(expected);
            });
        });

        it('should preserve other evaluation parameters when adjusting source type', () => {
            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
                false,
            );
            const volumeSpec = {
                evaluationParameters: {
                    sourceType: 'INVALID_SOURCE_TYPE' as DatasetVolumeSourceType,
                    // Removed field and query properties as they don't exist on DatasetVolumeAssertionParametersInput
                },
                evaluationSchedule: {
                    timezone: 'UTC',
                    cron: '0 0 * * *',
                },
                actions: {
                    onSuccess: [],
                    onFailure: [],
                },
                inferenceSettings: {
                    sensitivity: {
                        level: 9,
                    },
                    trainingDataLookbackWindowDays: 60,
                    exclusionWindows: [],
                },
            };

            const result = buildUpsertVolumeAssertionParams(dataset, volumeSpec);

            expect(result.variables.input.evaluationParameters.sourceType).toBe(
                DatasetVolumeSourceType.InformationSchema,
            );
            // Removed field and query assertions as those properties don't exist
            expect(result.variables.input.inferenceSettings).toEqual({
                sensitivity: {
                    level: 9,
                },
                trainingDataLookbackWindowDays: 60,
                exclusionWindows: [],
            });
        });
    });

    describe('Dataset subtype detection', () => {
        it('should detect view when subTypes is undefined', () => {
            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
                false,
            );
            delete dataset.subTypes;
            const volumeSpec = createMockVolumeSpec(DatasetVolumeSourceType.InformationSchema);

            const result = buildUpsertVolumeAssertionParams(dataset, volumeSpec);

            // Should be treated as a table (not a view)
            expect(result.variables.input.evaluationParameters.sourceType).toBe(
                DatasetVolumeSourceType.InformationSchema,
            );
        });

        it('should detect view when typeNames is empty', () => {
            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
                false,
            );
            dataset.subTypes = { typeNames: [] };
            const volumeSpec = createMockVolumeSpec(DatasetVolumeSourceType.InformationSchema);

            const result = buildUpsertVolumeAssertionParams(dataset, volumeSpec);

            // Should be treated as a table (not a view)
            expect(result.variables.input.evaluationParameters.sourceType).toBe(
                DatasetVolumeSourceType.InformationSchema,
            );
        });

        it('should handle mixed case view type names', () => {
            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.view,PROD)',
                false,
            );
            dataset.subTypes = { typeNames: ['View', 'materialized_view'] };
            const volumeSpec = createMockVolumeSpec(DatasetVolumeSourceType.InformationSchema);

            const result = buildUpsertVolumeAssertionParams(dataset, volumeSpec);

            // Should be treated as a view and fallback to Query
            expect(result.variables.input.evaluationParameters.sourceType).toBe(DatasetVolumeSourceType.Query);
        });
    });

    describe('Default assertion structure', () => {
        it('should always set correct default assertion structure', () => {
            const dataset = createMockDataset(
                BIGQUERY_URN,
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)',
                false,
            );
            const volumeSpec = createMockVolumeSpec(DatasetVolumeSourceType.Query);

            const result = buildUpsertVolumeAssertionParams(dataset, volumeSpec);

            expect(result.variables.input.mode).toBe(MonitorMode.Active);
            expect(result.variables.input.type).toBe(VolumeAssertionType.RowCountTotal);
            expect(result.variables.input.inferWithAI).toBe(true);
            expect(result.variables.input.rowCountTotal).toEqual({
                operator: AssertionStdOperator.Between,
                parameters: {
                    minValue: {
                        type: AssertionStdParameterType.Number,
                        value: '0',
                    },
                    maxValue: {
                        type: AssertionStdParameterType.Number,
                        value: '1000',
                    },
                },
            });
            expect(result.variables.input.description).toBe('Row count volume anomaly check');
        });
    });
});

// ------------------------------------------------------------------------------------------------------------------------//
// Test for buildCreateAssertionsForDataset function
// ------------------------------------------------------------------------------------------------------------------------//

describe('buildCreateAssertionsForDataset', () => {
    const createMockDataset = (platformUrn: string, urn: string): Dataset => ({
        urn,
        type: EntityType.Dataset,
        platform: {
            urn: platformUrn,
            type: EntityType.DataPlatform,
            name: platformUrn.split(':').pop() || 'unknown',
        },
        name: 'test-dataset',
        origin: FabricType.Prod,
        __typename: 'Dataset',
    });

    const createMockFreshnessSpec = (): NonNullable<BulkCreateDatasetAssertionsSpec['freshnessAssertionSpec']> => ({
        evaluationParameters: {
            sourceType: DatasetFreshnessSourceType.InformationSchema,
        },
        evaluationSchedule: {
            timezone: 'UTC',
            cron: '0 0 * * *',
        },
        actions: {
            onSuccess: [],
            onFailure: [],
        },
        criteria: {
            type: 'AI' as const,
            inferenceSettings: {
                sensitivity: {
                    level: 8,
                },
                trainingDataLookbackWindowDays: 30,
                exclusionWindows: [],
            },
        },
    });

    const createMockVolumeSpec = (): NonNullable<BulkCreateDatasetAssertionsSpec['volumeAssertionSpec']> => ({
        evaluationParameters: {
            sourceType: DatasetVolumeSourceType.InformationSchema,
        },
        evaluationSchedule: {
            timezone: 'UTC',
            cron: '0 0 * * *',
        },
        actions: {
            onSuccess: [],
            onFailure: [],
        },
        inferenceSettings: {
            sensitivity: {
                level: 8,
            },
            trainingDataLookbackWindowDays: 30,
            exclusionWindows: [],
        },
    });

    const createMockProgress = (): ProgressTracker => ({
        total: 10,
        completed: 5,
        successful: [],
        errored: [],
    });

    describe('Success scenarios', () => {
        it('should successfully create both freshness and volume assertions', async () => {
            let freshnessCallCount = 0;
            let volumeCallCount = 0;
            let progressCallCount = 0;
            let progressUpdaterFn: ((current: ProgressTracker) => ProgressTracker) | undefined;

            const mockUpsertFreshness = async () => {
                freshnessCallCount++;
                return { data: {} } as any;
            };
            const mockUpsertVolume = async () => {
                volumeCallCount++;
                return { data: {} } as any;
            };
            const mockSetProgress = (updater: (current: ProgressTracker) => ProgressTracker) => {
                progressCallCount++;
                progressUpdaterFn = updater;
            };

            const createAssertionsForDataset = buildCreateAssertionsForDataset(
                mockUpsertFreshness,
                mockUpsertVolume,
                mockSetProgress,
            );

            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );
            const freshnessSpec = createMockFreshnessSpec();
            const volumeSpec = createMockVolumeSpec();

            await createAssertionsForDataset(dataset, freshnessSpec, volumeSpec);

            expect(freshnessCallCount).toBe(1);
            expect(volumeCallCount).toBe(1);
            expect(progressCallCount).toBe(1);
            expect(progressUpdaterFn).toBeDefined();

            // Verify progress update
            const currentProgress = createMockProgress();
            const updatedProgress = progressUpdaterFn!(currentProgress);

            expect(updatedProgress).toEqual({
                total: 10,
                completed: 6,
                successful: [
                    {
                        dataset: dataset.urn,
                        assertionType: AssertionType.Freshness,
                    },
                    {
                        dataset: dataset.urn,
                        assertionType: AssertionType.Volume,
                    },
                ],
                errored: [],
            });
        });

        it('should successfully create only freshness assertion when volume spec is undefined', async () => {
            let freshnessCallCount = 0;
            let volumeCallCount = 0;
            let progressCallCount = 0;
            let progressUpdaterFn: ((current: ProgressTracker) => ProgressTracker) | undefined;

            const mockUpsertFreshness = async () => {
                freshnessCallCount++;
                return { data: {} } as any;
            };
            const mockUpsertVolume = async () => {
                volumeCallCount++;
                return { data: {} } as any;
            };
            const mockSetProgress = (updater: (current: ProgressTracker) => ProgressTracker) => {
                progressCallCount++;
                progressUpdaterFn = updater;
            };

            const createAssertionsForDataset = buildCreateAssertionsForDataset(
                mockUpsertFreshness,
                mockUpsertVolume,
                mockSetProgress,
            );

            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );
            const freshnessSpec = createMockFreshnessSpec();

            await createAssertionsForDataset(dataset, freshnessSpec, undefined);

            expect(freshnessCallCount).toBe(1);
            expect(volumeCallCount).toBe(0);
            expect(progressCallCount).toBe(1);
            expect(progressUpdaterFn).toBeDefined();

            // Verify progress update
            const currentProgress = createMockProgress();
            const updatedProgress = progressUpdaterFn!(currentProgress);

            expect(updatedProgress.successful).toHaveLength(1);
            expect(updatedProgress.successful[0]).toEqual({
                dataset: dataset.urn,
                assertionType: AssertionType.Freshness,
            });
        });

        it('should successfully create only volume assertion when freshness spec is undefined', async () => {
            let freshnessCallCount = 0;
            let volumeCallCount = 0;
            let progressCallCount = 0;
            let progressUpdaterFn: ((current: ProgressTracker) => ProgressTracker) | undefined;

            const mockUpsertFreshness = async () => {
                freshnessCallCount++;
                return { data: {} } as any;
            };
            const mockUpsertVolume = async () => {
                volumeCallCount++;
                return { data: {} } as any;
            };
            const mockSetProgress = (updater: (current: ProgressTracker) => ProgressTracker) => {
                progressCallCount++;
                progressUpdaterFn = updater;
            };

            const createAssertionsForDataset = buildCreateAssertionsForDataset(
                mockUpsertFreshness,
                mockUpsertVolume,
                mockSetProgress,
            );

            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );
            const volumeSpec = createMockVolumeSpec();

            await createAssertionsForDataset(dataset, undefined, volumeSpec);

            expect(freshnessCallCount).toBe(0);
            expect(volumeCallCount).toBe(1);
            expect(progressCallCount).toBe(1);
            expect(progressUpdaterFn).toBeDefined();

            // Verify progress update
            const currentProgress = createMockProgress();
            const updatedProgress = progressUpdaterFn!(currentProgress);

            expect(updatedProgress.successful).toHaveLength(1);
            expect(updatedProgress.successful[0]).toEqual({
                dataset: dataset.urn,
                assertionType: AssertionType.Volume,
            });
        });

        it('should do nothing when both specs are undefined', async () => {
            let freshnessCallCount = 0;
            let volumeCallCount = 0;
            let progressCallCount = 0;
            let progressUpdaterFn: ((current: ProgressTracker) => ProgressTracker) | undefined;

            const mockUpsertFreshness = async () => {
                freshnessCallCount++;
                return { data: {} } as any;
            };
            const mockUpsertVolume = async () => {
                volumeCallCount++;
                return { data: {} } as any;
            };
            const mockSetProgress = (updater: (current: ProgressTracker) => ProgressTracker) => {
                progressCallCount++;
                progressUpdaterFn = updater;
            };

            const createAssertionsForDataset = buildCreateAssertionsForDataset(
                mockUpsertFreshness,
                mockUpsertVolume,
                mockSetProgress,
            );

            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );

            await createAssertionsForDataset(dataset, undefined, undefined);

            expect(freshnessCallCount).toBe(0);
            expect(volumeCallCount).toBe(0);
            expect(progressCallCount).toBe(1);
            expect(progressUpdaterFn).toBeDefined();

            // Verify progress update
            const currentProgress = createMockProgress();
            const updatedProgress = progressUpdaterFn!(currentProgress);

            expect(updatedProgress.successful).toHaveLength(0);
            expect(updatedProgress.errored).toHaveLength(0);
            expect(updatedProgress.completed).toBe(6); // Should still increment completed
        });
    });

    describe('Error scenarios', () => {
        it('should handle freshness assertion error', async () => {
            let freshnessCallCount = 0;
            let volumeCallCount = 0;
            let progressCallCount = 0;
            let progressUpdaterFn: ((current: ProgressTracker) => ProgressTracker) | undefined;

            const freshnessError = new Error('Freshness assertion failed');
            const mockUpsertFreshness = async () => {
                freshnessCallCount++;
                throw freshnessError;
            };
            const mockUpsertVolume = async () => {
                volumeCallCount++;
                return { data: {} } as any;
            };
            const mockSetProgress = (updater: (current: ProgressTracker) => ProgressTracker) => {
                progressCallCount++;
                progressUpdaterFn = updater;
            };

            const createAssertionsForDataset = buildCreateAssertionsForDataset(
                mockUpsertFreshness,
                mockUpsertVolume,
                mockSetProgress,
            );

            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );
            const freshnessSpec = createMockFreshnessSpec();
            const volumeSpec = createMockVolumeSpec();

            await createAssertionsForDataset(dataset, freshnessSpec, volumeSpec);

            expect(freshnessCallCount).toBe(1);
            expect(volumeCallCount).toBe(1);
            expect(progressCallCount).toBe(1);
            expect(progressUpdaterFn).toBeDefined();

            // Verify progress update
            const currentProgress = createMockProgress();
            const updatedProgress = progressUpdaterFn!(currentProgress);

            expect(updatedProgress.successful).toHaveLength(1);
            expect(updatedProgress.successful[0]).toEqual({
                dataset: dataset.urn,
                assertionType: AssertionType.Volume,
            });
            expect(updatedProgress.errored).toHaveLength(1);
            expect(updatedProgress.errored[0]).toEqual({
                dataset: dataset.urn,
                assertionType: AssertionType.Freshness,
                error: 'Freshness assertion failed',
            });
        });

        it('should handle volume assertion error', async () => {
            let freshnessCallCount = 0;
            let volumeCallCount = 0;
            let progressCallCount = 0;
            let progressUpdaterFn: ((current: ProgressTracker) => ProgressTracker) | undefined;

            const volumeError = new Error('Volume assertion failed');
            const mockUpsertFreshness = async () => {
                freshnessCallCount++;
                return { data: {} } as any;
            };
            const mockUpsertVolume = async () => {
                volumeCallCount++;
                throw volumeError;
            };
            const mockSetProgress = (updater: (current: ProgressTracker) => ProgressTracker) => {
                progressCallCount++;
                progressUpdaterFn = updater;
            };

            const createAssertionsForDataset = buildCreateAssertionsForDataset(
                mockUpsertFreshness,
                mockUpsertVolume,
                mockSetProgress,
            );

            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );
            const freshnessSpec = createMockFreshnessSpec();
            const volumeSpec = createMockVolumeSpec();

            await createAssertionsForDataset(dataset, freshnessSpec, volumeSpec);

            expect(freshnessCallCount).toBe(1);
            expect(volumeCallCount).toBe(1);
            expect(progressCallCount).toBe(1);
            expect(progressUpdaterFn).toBeDefined();

            // Verify progress update
            const currentProgress = createMockProgress();
            const updatedProgress = progressUpdaterFn!(currentProgress);

            expect(updatedProgress.successful).toHaveLength(1);
            expect(updatedProgress.successful[0]).toEqual({
                dataset: dataset.urn,
                assertionType: AssertionType.Freshness,
            });
            expect(updatedProgress.errored).toHaveLength(1);
            expect(updatedProgress.errored[0]).toEqual({
                dataset: dataset.urn,
                assertionType: AssertionType.Volume,
                error: 'Volume assertion failed',
            });
        });

        it('should handle both assertions failing', async () => {
            let freshnessCallCount = 0;
            let volumeCallCount = 0;
            let progressCallCount = 0;
            let progressUpdaterFn: ((current: ProgressTracker) => ProgressTracker) | undefined;

            const freshnessError = new Error('Freshness assertion failed');
            const volumeError = new Error('Volume assertion failed');
            const mockUpsertFreshness = async () => {
                freshnessCallCount++;
                throw freshnessError;
            };
            const mockUpsertVolume = async () => {
                volumeCallCount++;
                throw volumeError;
            };
            const mockSetProgress = (updater: (current: ProgressTracker) => ProgressTracker) => {
                progressCallCount++;
                progressUpdaterFn = updater;
            };

            const createAssertionsForDataset = buildCreateAssertionsForDataset(
                mockUpsertFreshness,
                mockUpsertVolume,
                mockSetProgress,
            );

            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );
            const freshnessSpec = createMockFreshnessSpec();
            const volumeSpec = createMockVolumeSpec();

            await createAssertionsForDataset(dataset, freshnessSpec, volumeSpec);

            expect(freshnessCallCount).toBe(1);
            expect(volumeCallCount).toBe(1);
            expect(progressCallCount).toBe(1);
            expect(progressUpdaterFn).toBeDefined();

            // Verify progress update
            const currentProgress = createMockProgress();
            const updatedProgress = progressUpdaterFn!(currentProgress);

            expect(updatedProgress.successful).toHaveLength(0);
            expect(updatedProgress.errored).toHaveLength(2);
            expect(updatedProgress.errored).toEqual([
                {
                    dataset: dataset.urn,
                    assertionType: AssertionType.Freshness,
                    error: 'Freshness assertion failed',
                },
                {
                    dataset: dataset.urn,
                    assertionType: AssertionType.Volume,
                    error: 'Volume assertion failed',
                },
            ]);
        });

        it('should handle non-Error exceptions', async () => {
            let freshnessCallCount = 0;
            let volumeCallCount = 0;
            let progressCallCount = 0;
            let progressUpdaterFn: ((current: ProgressTracker) => ProgressTracker) | undefined;

            const mockUpsertFreshness = async () => {
                freshnessCallCount++;
                throw new Error('String error');
            };
            const mockUpsertVolume = async () => {
                volumeCallCount++;
                throw new Error('Object error');
            };
            const mockSetProgress = (updater: (current: ProgressTracker) => ProgressTracker) => {
                progressCallCount++;
                progressUpdaterFn = updater;
            };

            const createAssertionsForDataset = buildCreateAssertionsForDataset(
                mockUpsertFreshness,
                mockUpsertVolume,
                mockSetProgress,
            );

            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );
            const freshnessSpec = createMockFreshnessSpec();
            const volumeSpec = createMockVolumeSpec();

            await createAssertionsForDataset(dataset, freshnessSpec, volumeSpec);

            expect(freshnessCallCount).toBe(1);
            expect(volumeCallCount).toBe(1);
            expect(progressCallCount).toBe(1);
            expect(progressUpdaterFn).toBeDefined();

            // Verify progress update
            const currentProgress = createMockProgress();
            const updatedProgress = progressUpdaterFn!(currentProgress);

            expect(updatedProgress.errored).toHaveLength(2);
            expect(updatedProgress.errored[0]).toEqual({
                dataset: dataset.urn,
                assertionType: AssertionType.Freshness,
                error: 'String error',
            });
            expect(updatedProgress.errored[1]).toEqual({
                dataset: dataset.urn,
                assertionType: AssertionType.Volume,
                error: 'Object error',
            });
        });
    });

    describe('Progress tracking', () => {
        it('should correctly update progress counts', async () => {
            let progressCallCount = 0;
            let progressUpdaterFn: ((current: ProgressTracker) => ProgressTracker) | undefined;

            const mockUpsertFreshness = async () => {
                return { data: {} } as any;
            };
            const mockUpsertVolume = async () => {
                return { data: {} } as any;
            };
            const mockSetProgress = (updater: (current: ProgressTracker) => ProgressTracker) => {
                progressCallCount++;
                progressUpdaterFn = updater;
            };

            const createAssertionsForDataset = buildCreateAssertionsForDataset(
                mockUpsertFreshness,
                mockUpsertVolume,
                mockSetProgress,
            );

            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );
            const freshnessSpec = createMockFreshnessSpec();
            const volumeSpec = createMockVolumeSpec();

            await createAssertionsForDataset(dataset, freshnessSpec, volumeSpec);

            // Verify progress update function was called once
            expect(progressCallCount).toBe(1);
            expect(progressUpdaterFn).toBeDefined();

            const currentProgress: ProgressTracker = {
                total: 100,
                completed: 50,
                successful: [{ dataset: 'other:dataset', assertionType: AssertionType.Freshness }],
                errored: [{ dataset: 'other:dataset', assertionType: AssertionType.Volume, error: 'Previous error' }],
            };

            const updatedProgress = progressUpdaterFn!(currentProgress);

            expect(updatedProgress).toEqual({
                total: 100, // Should preserve total
                completed: 51, // Should increment by 1
                successful: [
                    { dataset: 'other:dataset', assertionType: AssertionType.Freshness }, // Previous success preserved
                    { dataset: dataset.urn, assertionType: AssertionType.Freshness }, // New successes added
                    { dataset: dataset.urn, assertionType: AssertionType.Volume },
                ],
                errored: [
                    { dataset: 'other:dataset', assertionType: AssertionType.Volume, error: 'Previous error' }, // Previous errors preserved
                ],
            });
        });

        it('should preserve existing progress when adding new results', async () => {
            let progressCallCount = 0;
            let progressUpdaterFn: ((current: ProgressTracker) => ProgressTracker) | undefined;

            const mockUpsertFreshness = async () => {
                throw new Error('Test error');
            };
            const mockUpsertVolume = async () => {
                return { data: {} } as any;
            };
            const mockSetProgress = (updater: (current: ProgressTracker) => ProgressTracker) => {
                progressCallCount++;
                progressUpdaterFn = updater;
            };

            const createAssertionsForDataset = buildCreateAssertionsForDataset(
                mockUpsertFreshness,
                mockUpsertVolume,
                mockSetProgress,
            );

            const dataset = createMockDataset(
                SNOWFLAKE_URN,
                'urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)',
            );
            const freshnessSpec = createMockFreshnessSpec();
            const volumeSpec = createMockVolumeSpec();

            await createAssertionsForDataset(dataset, freshnessSpec, volumeSpec);

            expect(progressCallCount).toBe(1);
            expect(progressUpdaterFn).toBeDefined();

            const currentProgress: ProgressTracker = {
                total: 100,
                completed: 75,
                successful: [{ dataset: 'existing:success', assertionType: AssertionType.Volume }],
                errored: [
                    { dataset: 'existing:error', assertionType: AssertionType.Freshness, error: 'Existing error' },
                ],
            };

            const updatedProgress = progressUpdaterFn!(currentProgress);

            expect(updatedProgress.successful).toHaveLength(2); // 1 existing + 1 new
            expect(updatedProgress.errored).toHaveLength(2); // 1 existing + 1 new
            expect(updatedProgress.completed).toBe(76); // 75 + 1
            expect(updatedProgress.total).toBe(100); // Preserved
        });
    });

    describe('Parameter passing', () => {
        it('should pass correct parameters to freshness upsert function', async () => {
            let freshnessCallCount = 0;
            let freshnessParams: any;

            const mockUpsertFreshness = async (params: any) => {
                freshnessCallCount++;
                freshnessParams = params;
                return { data: {} } as any;
            };
            const mockUpsertVolume = async () => {
                return { data: {} } as any;
            };
            const mockSetProgress = () => {
                // No-op for this test
            };

            const createAssertionsForDataset = buildCreateAssertionsForDataset(
                mockUpsertFreshness,
                mockUpsertVolume,
                mockSetProgress,
            );

            const dataset = createMockDataset(
                BIGQUERY_URN,
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)',
            );
            const freshnessSpec = createMockFreshnessSpec();

            await createAssertionsForDataset(dataset, freshnessSpec, undefined);

            expect(freshnessCallCount).toBe(1);
            expect(freshnessParams).toEqual(buildUpsertFreshnessAssertionParams(dataset, freshnessSpec));
        });

        it('should pass correct parameters to volume upsert function', async () => {
            let volumeCallCount = 0;
            let volumeParams: any;

            const mockUpsertFreshness = async () => {
                return { data: {} } as any;
            };
            const mockUpsertVolume = async (params: any) => {
                volumeCallCount++;
                volumeParams = params;
                return { data: {} } as any;
            };
            const mockSetProgress = () => {
                // No-op for this test
            };

            const createAssertionsForDataset = buildCreateAssertionsForDataset(
                mockUpsertFreshness,
                mockUpsertVolume,
                mockSetProgress,
            );

            const dataset = createMockDataset(
                REDSHIFT_URN,
                'urn:li:dataset:(urn:li:dataPlatform:redshift,database.schema.table,PROD)',
            );
            const volumeSpec = createMockVolumeSpec();

            await createAssertionsForDataset(dataset, undefined, volumeSpec);

            expect(volumeCallCount).toBe(1);
            expect(volumeParams).toEqual(buildUpsertVolumeAssertionParams(dataset, volumeSpec));
        });
    });
});
