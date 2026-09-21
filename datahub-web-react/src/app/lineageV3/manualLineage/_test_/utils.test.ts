import { filterManualLineageUrns, getValidEntityTypes } from '@app/lineageV3/manualLineage/utils';

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

describe('filterManualLineageUrns', () => {
    const chartUrn = 'urn:li:chart:(looker,orders_chart)';
    const dashboardUrn = 'urn:li:dashboard:(looker,orders_dashboard)';
    const metricUrn = 'urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,double_revenue)';

    it('omits a Metric neighbor when Metric is not persistable in this direction', () => {
        const result = filterManualLineageUrns([chartUrn, metricUrn, dashboardUrn], [
            EntityType.Dataset,
            EntityType.Chart,
            EntityType.Dashboard,
        ]);
        expect(result).toStrictEqual([chartUrn, dashboardUrn]);
    });

    it('keeps a Metric neighbor when Metric is a valid upstream of the home entity', () => {
        const result = filterManualLineageUrns([chartUrn, metricUrn], [EntityType.Chart, EntityType.Metric]);
        expect(result).toStrictEqual([chartUrn, metricUrn]);
    });
});
