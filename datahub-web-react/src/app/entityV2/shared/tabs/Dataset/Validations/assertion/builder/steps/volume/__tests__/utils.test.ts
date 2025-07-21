import { getVolumeSourceTypeOptions } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/volume/utils';
import { BIGQUERY_URN, DATABRICKS_URN, REDSHIFT_URN, SNOWFLAKE_URN } from '@app/ingest/source/builder/constants';

import { DatasetVolumeSourceType } from '@types';

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
