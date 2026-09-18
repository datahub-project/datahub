import { getValidEntityTypes } from '@app/lineageV3/manualLineage/utils';

import { EntityType, LineageDirection } from '@types';

describe('getValidEntityTypes', () => {
    describe('For Downstream', () => {
        it('should return DASHBOARD if Chart Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Downstream, EntityType.Chart);
            expect(result).toStrictEqual(['DASHBOARD']);
        });

        it('should return DATASET,CHART,DASHBOARD, DATA_JOB, METRIC if Dataset Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Downstream, EntityType.Dataset);
            expect(result).toStrictEqual(['DATASET', 'CHART', 'DASHBOARD', 'DATA_JOB', 'METRIC']);
        });

        it('should return DATASET, CHART, DASHBOARD if Metric Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Downstream, EntityType.Metric);
            expect(result).toStrictEqual(['DATASET', 'CHART', 'DASHBOARD']);
        });

        it('should return DATASET, DATA_JOB if DataJob Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Downstream, EntityType.DataJob);
            expect(result).toStrictEqual(['DATA_JOB', 'DATASET']);
        });

        it('should return empty Array if DataJob Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Downstream, EntityType.Dashboard);
            expect(result).toStrictEqual([]);
        });

        it('should return empty Array if empty Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Downstream);
            expect(result).toStrictEqual([]);
        });
    });

    describe('For UpStream', () => {
        it('should return DATASET and METRIC if Chart Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Upstream, EntityType.Chart);
            expect(result).toStrictEqual(['DATASET', 'METRIC']);
        });

        it('should return DATASET, DATA_JOB and METRIC if Dataset Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Upstream, EntityType.Dataset);
            expect(result).toStrictEqual(['DATASET', 'DATA_JOB', 'METRIC']);
        });

        it('should return empty Array if Metric Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Upstream, EntityType.Metric);
            expect(result).toStrictEqual([]);
        });

        it('should return DATASET and DATA_JOB if DataJob Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Upstream, EntityType.DataJob);
            expect(result).toStrictEqual(['DATA_JOB', 'DATASET']);
        });

        it('should return CHART, DATASET and METRIC if Dashboard Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Upstream, EntityType.Dashboard);
            expect(result).toStrictEqual(['CHART', 'DATASET', 'METRIC']);
        });

        it('should return empty Array if empty Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Upstream);
            expect(result).toStrictEqual([]);
        });
    });
});
