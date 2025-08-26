import {
    getDefaultDatasetVolumeAssertionParametersState,
    getDefaultVolumeSourceType,
    getVolumeSourceTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/utils';
import { BIGQUERY_URN, DATABRICKS_URN, REDSHIFT_URN, SNOWFLAKE_URN } from '@app/ingest/source/builder/constants';

import { AssertionEvaluationParametersType, DatasetVolumeSourceType } from '@types';

describe('getVolumeSourceTypeOptions', () => {
    describe('when connectionForEntityExists is false', () => {
        it('should return only DatahubDatasetProfile regardless of platform or view type', () => {
            const result = getVolumeSourceTypeOptions(SNOWFLAKE_URN, false, false);
            expect(result).toEqual([DatasetVolumeSourceType.DatahubDatasetProfile]);
        });

        it('should return only DatahubDatasetProfile for views when connection does not exist', () => {
            const result = getVolumeSourceTypeOptions(BIGQUERY_URN, false, true);
            expect(result).toEqual([DatasetVolumeSourceType.DatahubDatasetProfile]);
        });

        it('should return only DatahubDatasetProfile for unknown platform when connection does not exist', () => {
            const result = getVolumeSourceTypeOptions('unknown-platform', false, false);
            expect(result).toEqual([DatasetVolumeSourceType.DatahubDatasetProfile]);
        });
    });

    describe('when connectionForEntityExists is true', () => {
        describe('for Snowflake', () => {
            it('should return all source types for tables', () => {
                const result = getVolumeSourceTypeOptions(SNOWFLAKE_URN, true, false);
                expect(result).toEqual([
                    DatasetVolumeSourceType.InformationSchema,
                    DatasetVolumeSourceType.Query,
                    DatasetVolumeSourceType.DatahubDatasetProfile,
                ]);
            });

            it('should exclude InformationSchema for views', () => {
                const result = getVolumeSourceTypeOptions(SNOWFLAKE_URN, true, true);
                expect(result).toEqual([DatasetVolumeSourceType.Query, DatasetVolumeSourceType.DatahubDatasetProfile]);
            });
        });

        describe('for BigQuery', () => {
            it('should return all source types for tables', () => {
                const result = getVolumeSourceTypeOptions(BIGQUERY_URN, true, false);
                expect(result).toEqual([
                    DatasetVolumeSourceType.InformationSchema,
                    DatasetVolumeSourceType.Query,
                    DatasetVolumeSourceType.DatahubDatasetProfile,
                ]);
            });

            it('should exclude InformationSchema for views', () => {
                const result = getVolumeSourceTypeOptions(BIGQUERY_URN, true, true);
                expect(result).toEqual([DatasetVolumeSourceType.Query, DatasetVolumeSourceType.DatahubDatasetProfile]);
            });
        });

        describe('for Redshift', () => {
            it('should return all source types for tables', () => {
                const result = getVolumeSourceTypeOptions(REDSHIFT_URN, true, false);
                expect(result).toEqual([
                    DatasetVolumeSourceType.InformationSchema,
                    DatasetVolumeSourceType.Query,
                    DatasetVolumeSourceType.DatahubDatasetProfile,
                ]);
            });

            it('should exclude InformationSchema for views', () => {
                const result = getVolumeSourceTypeOptions(REDSHIFT_URN, true, true);
                expect(result).toEqual([DatasetVolumeSourceType.Query, DatasetVolumeSourceType.DatahubDatasetProfile]);
            });
        });

        describe('for Databricks', () => {
            it('should return configured source types for tables', () => {
                const result = getVolumeSourceTypeOptions(DATABRICKS_URN, true, false);
                expect(result).toEqual([DatasetVolumeSourceType.Query, DatasetVolumeSourceType.DatahubDatasetProfile]);
            });

            it('should return same source types for views (Databricks does not include InformationSchema)', () => {
                const result = getVolumeSourceTypeOptions(DATABRICKS_URN, true, true);
                expect(result).toEqual([DatasetVolumeSourceType.Query, DatasetVolumeSourceType.DatahubDatasetProfile]);
            });
        });

        describe('for unknown platform', () => {
            it('should return default DatahubDatasetProfile for unknown platform', () => {
                const result = getVolumeSourceTypeOptions('unknown-platform-urn', true, false);
                expect(result).toEqual([DatasetVolumeSourceType.DatahubDatasetProfile]);
            });

            it('should return default DatahubDatasetProfile for unknown platform view', () => {
                const result = getVolumeSourceTypeOptions('unknown-platform-urn', true, true);
                expect(result).toEqual([DatasetVolumeSourceType.DatahubDatasetProfile]);
            });
        });
    });

    describe('edge cases', () => {
        it('should handle empty platform URN', () => {
            const result = getVolumeSourceTypeOptions('', true, false);
            expect(result).toEqual([DatasetVolumeSourceType.DatahubDatasetProfile]);
        });

        it('should handle null/undefined platform URN gracefully', () => {
            const result = getVolumeSourceTypeOptions(null as any, true, false);
            expect(result).toEqual([DatasetVolumeSourceType.DatahubDatasetProfile]);
        });
    });
});

describe('getDefaultVolumeSourceType', () => {
    describe('when connectionForEntityExists is false', () => {
        it('should return DatahubDatasetProfile regardless of platform or isView', () => {
            expect(getDefaultVolumeSourceType(SNOWFLAKE_URN, false, false)).toBe(
                DatasetVolumeSourceType.DatahubDatasetProfile,
            );
            expect(getDefaultVolumeSourceType(SNOWFLAKE_URN, false, true)).toBe(
                DatasetVolumeSourceType.DatahubDatasetProfile,
            );
            expect(getDefaultVolumeSourceType(BIGQUERY_URN, false, false)).toBe(
                DatasetVolumeSourceType.DatahubDatasetProfile,
            );
            expect(getDefaultVolumeSourceType(BIGQUERY_URN, false, true)).toBe(
                DatasetVolumeSourceType.DatahubDatasetProfile,
            );
        });
    });

    describe('when connectionForEntityExists is true', () => {
        describe('for platforms with InformationSchema as default', () => {
            it('should return InformationSchema for tables', () => {
                expect(getDefaultVolumeSourceType(SNOWFLAKE_URN, true, false)).toBe(
                    DatasetVolumeSourceType.InformationSchema,
                );
                expect(getDefaultVolumeSourceType(BIGQUERY_URN, true, false)).toBe(
                    DatasetVolumeSourceType.InformationSchema,
                );
                expect(getDefaultVolumeSourceType(REDSHIFT_URN, true, false)).toBe(
                    DatasetVolumeSourceType.InformationSchema,
                );
            });

            it('should return Query for views (fallback from InformationSchema)', () => {
                expect(getDefaultVolumeSourceType(SNOWFLAKE_URN, true, true)).toBe(DatasetVolumeSourceType.Query);
                expect(getDefaultVolumeSourceType(BIGQUERY_URN, true, true)).toBe(DatasetVolumeSourceType.Query);
                expect(getDefaultVolumeSourceType(REDSHIFT_URN, true, true)).toBe(DatasetVolumeSourceType.Query);
            });

            it('should handle undefined isView parameter (should work the same as false)', () => {
                expect(getDefaultVolumeSourceType(SNOWFLAKE_URN, true, undefined)).toBe(
                    DatasetVolumeSourceType.InformationSchema,
                );
                expect(getDefaultVolumeSourceType(BIGQUERY_URN, true, undefined)).toBe(
                    DatasetVolumeSourceType.InformationSchema,
                );
                expect(getDefaultVolumeSourceType(REDSHIFT_URN, true, undefined)).toBe(
                    DatasetVolumeSourceType.InformationSchema,
                );
            });
        });

        describe('for Databricks (Query as default)', () => {
            it('should return Query for both tables and views', () => {
                expect(getDefaultVolumeSourceType(DATABRICKS_URN, true, false)).toBe(DatasetVolumeSourceType.Query);
                expect(getDefaultVolumeSourceType(DATABRICKS_URN, true, true)).toBe(DatasetVolumeSourceType.Query);
                expect(getDefaultVolumeSourceType(DATABRICKS_URN, true, undefined)).toBe(DatasetVolumeSourceType.Query);
            });
        });

        describe('for unknown platform', () => {
            it('should return DatahubDatasetProfile for unknown platforms', () => {
                expect(getDefaultVolumeSourceType('unknown-platform', true, false)).toBe(
                    DatasetVolumeSourceType.DatahubDatasetProfile,
                );
                expect(getDefaultVolumeSourceType('unknown-platform', true, true)).toBe(
                    DatasetVolumeSourceType.DatahubDatasetProfile,
                );
            });
        });
    });
});

describe('getDefaultDatasetVolumeAssertionParametersState', () => {
    describe('when monitorsConnectionForEntityExists is false', () => {
        it('should return DatahubDatasetProfile source type regardless of platform or isView', () => {
            const result = getDefaultDatasetVolumeAssertionParametersState(SNOWFLAKE_URN, false, false);
            expect(result).toEqual({
                type: AssertionEvaluationParametersType.DatasetVolume,
                datasetVolumeParameters: {
                    sourceType: DatasetVolumeSourceType.DatahubDatasetProfile,
                },
            });

            const resultView = getDefaultDatasetVolumeAssertionParametersState(SNOWFLAKE_URN, false, true);
            expect(resultView).toEqual({
                type: AssertionEvaluationParametersType.DatasetVolume,
                datasetVolumeParameters: {
                    sourceType: DatasetVolumeSourceType.DatahubDatasetProfile,
                },
            });
        });
    });

    describe('when monitorsConnectionForEntityExists is true', () => {
        describe('for platforms with InformationSchema as default', () => {
            it('should return InformationSchema source type for tables', () => {
                const result = getDefaultDatasetVolumeAssertionParametersState(SNOWFLAKE_URN, true, false);
                expect(result).toEqual({
                    type: AssertionEvaluationParametersType.DatasetVolume,
                    datasetVolumeParameters: {
                        sourceType: DatasetVolumeSourceType.InformationSchema,
                    },
                });
            });

            it('should return Query source type for views (fallback from InformationSchema)', () => {
                const result = getDefaultDatasetVolumeAssertionParametersState(SNOWFLAKE_URN, true, true);
                expect(result).toEqual({
                    type: AssertionEvaluationParametersType.DatasetVolume,
                    datasetVolumeParameters: {
                        sourceType: DatasetVolumeSourceType.Query,
                    },
                });

                const resultBigQuery = getDefaultDatasetVolumeAssertionParametersState(BIGQUERY_URN, true, true);
                expect(resultBigQuery).toEqual({
                    type: AssertionEvaluationParametersType.DatasetVolume,
                    datasetVolumeParameters: {
                        sourceType: DatasetVolumeSourceType.Query,
                    },
                });
            });

            it('should handle undefined isView parameter', () => {
                const result = getDefaultDatasetVolumeAssertionParametersState(SNOWFLAKE_URN, true, undefined);
                expect(result).toEqual({
                    type: AssertionEvaluationParametersType.DatasetVolume,
                    datasetVolumeParameters: {
                        sourceType: DatasetVolumeSourceType.InformationSchema,
                    },
                });
            });
        });

        describe('for Databricks (Query as default)', () => {
            it('should return Query source type for both tables and views', () => {
                const resultTable = getDefaultDatasetVolumeAssertionParametersState(DATABRICKS_URN, true, false);
                expect(resultTable).toEqual({
                    type: AssertionEvaluationParametersType.DatasetVolume,
                    datasetVolumeParameters: {
                        sourceType: DatasetVolumeSourceType.Query,
                    },
                });

                const resultView = getDefaultDatasetVolumeAssertionParametersState(DATABRICKS_URN, true, true);
                expect(resultView).toEqual({
                    type: AssertionEvaluationParametersType.DatasetVolume,
                    datasetVolumeParameters: {
                        sourceType: DatasetVolumeSourceType.Query,
                    },
                });
            });
        });

        describe('for unknown platform', () => {
            it('should return DatahubDatasetProfile for unknown platforms', () => {
                const result = getDefaultDatasetVolumeAssertionParametersState('unknown-platform', true, false);
                expect(result).toEqual({
                    type: AssertionEvaluationParametersType.DatasetVolume,
                    datasetVolumeParameters: {
                        sourceType: DatasetVolumeSourceType.DatahubDatasetProfile,
                    },
                });
            });
        });
    });
});
