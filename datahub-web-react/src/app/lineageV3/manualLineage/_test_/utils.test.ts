import { getValidEntityTypes } from '@app/lineageV3/manualLineage/utils';

import { EntityType, LineageDirection } from '@types';

describe('getValidEntityTypes', () => {
    describe('For Downstream', () => {
        it('should return DASHBOARD if Chart Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Downstream, EntityType.Chart);
            expect(result).toStrictEqual(['DASHBOARD']);
        });

        it('should return DATASET,CHART,DASHBOARD, DATA_JOB if Dataset Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Downstream, EntityType.Dataset);
            expect(result).toStrictEqual(['DATASET', 'CHART', 'DASHBOARD', 'DATA_JOB']);
        });

        it('should return DATA_JOB, DATASET, MLMODEL, MLMODEL_GROUP if DataJob Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Downstream, EntityType.DataJob);
            expect(result).toStrictEqual(['DATA_JOB', 'DATASET', 'MLMODEL', 'MLMODEL_GROUP']);
        });

        it('should return DATA_JOB if Mlmodel Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Downstream, EntityType.Mlmodel);
            expect(result).toStrictEqual(['DATA_JOB']);
        });

        it('should return DATA_JOB if MlmodelGroup Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Downstream, EntityType.MlmodelGroup);
            expect(result).toStrictEqual(['DATA_JOB']);
        });

        it('should return empty Array if Dashboard Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Downstream, EntityType.Dashboard);
            expect(result).toStrictEqual([]);
        });

        it('should return empty Array if empty Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Downstream);
            expect(result).toStrictEqual([]);
        });
    });

    describe('For UpStream', () => {
        it('should return DATASET if Chart Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Upstream, EntityType.Chart);
            expect(result).toStrictEqual(['DATASET']);
        });

        it('should return DATASET and DATA_JOB if Dataset Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Upstream, EntityType.Dataset);
            expect(result).toStrictEqual(['DATASET', 'DATA_JOB']);
        });

        it('should return DATA_JOB, DATASET, MLMODEL, MLMODEL_GROUP if DataJob Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Upstream, EntityType.DataJob);
            expect(result).toStrictEqual(['DATA_JOB', 'DATASET', 'MLMODEL', 'MLMODEL_GROUP']);
        });

        it('should return DATA_JOB, DATA_PROCESS_INSTANCE if Mlmodel Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Upstream, EntityType.Mlmodel);
            expect(result).toStrictEqual(['DATA_JOB', 'DATA_PROCESS_INSTANCE']);
        });

        it('should return DATA_JOB if MlmodelGroup Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Upstream, EntityType.MlmodelGroup);
            expect(result).toStrictEqual(['DATA_JOB']);
        });

        it('should return CHART and DATASET Array if Dashboard Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Upstream, EntityType.Dashboard);
            expect(result).toStrictEqual(['CHART', 'DATASET']);
        });

        it('should return empty Array if empty Entity type is passed', () => {
            const result = getValidEntityTypes(LineageDirection.Upstream);
            expect(result).toStrictEqual([]);
        });
    });
});
