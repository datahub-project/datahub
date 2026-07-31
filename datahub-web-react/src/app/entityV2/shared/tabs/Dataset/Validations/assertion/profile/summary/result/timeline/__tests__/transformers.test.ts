import { describe, expect, it } from 'vitest';

import { getAssertionResultChartData } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/result/timeline/transformers';

import {
    Assertion,
    AssertionResultType,
    AssertionRunEvent,
    AssertionType,
    FieldAssertionType,
    FieldMetricType,
} from '@types';

// ---------------------------------------------------------------------------
// Factories
// ---------------------------------------------------------------------------

const makeAssertion = (infoOverride?: Partial<Assertion['info']>): Assertion =>
    ({
        urn: 'urn:li:assertion:test',
        type: 'ASSERTION' as any,
        platform: { urn: 'urn:li:dataPlatform:snowflake', type: 'DATA_PLATFORM' as any },
        info: infoOverride ? ({ ...infoOverride } as any) : undefined,
    }) as Assertion;

const makeRun = (timestampMillis: number, resultType?: AssertionResultType, externalUrl?: string): AssertionRunEvent =>
    ({
        asserteeUrn: 'urn:li:dataset:test',
        assertionUrn: 'urn:li:assertion:test',
        runId: 'run-1',
        status: 'COMPLETE' as any,
        timestampMillis,
        result: resultType ? { type: resultType, externalUrl: externalUrl ?? null } : undefined,
    }) as AssertionRunEvent;

// ---------------------------------------------------------------------------
// getAssertionResultChartData — data points
// ---------------------------------------------------------------------------

describe('getAssertionResultChartData — data points', () => {
    it('returns empty data points for an empty run list', () => {
        const { dataPoints } = getAssertionResultChartData(makeAssertion({ type: AssertionType.Volume }), []);
        expect(dataPoints).toHaveLength(0);
    });

    it('filters out run events with no result', () => {
        const runs = [makeRun(1000), makeRun(2000, AssertionResultType.Success)];
        const { dataPoints } = getAssertionResultChartData(makeAssertion({ type: AssertionType.Volume }), runs);
        expect(dataPoints).toHaveLength(1);
        expect(dataPoints[0].time).toBe(2000);
    });

    it('maps each run event with a result to a data point', () => {
        const runs = [
            makeRun(1000, AssertionResultType.Success),
            makeRun(2000, AssertionResultType.Failure),
            makeRun(3000, AssertionResultType.Error),
        ];
        const { dataPoints } = getAssertionResultChartData(makeAssertion({ type: AssertionType.Volume }), runs);
        expect(dataPoints).toHaveLength(3);
    });

    it('sets time, result.type, and relatedRunEvent correctly on each data point', () => {
        const run = makeRun(5000, AssertionResultType.Failure, 'https://example.com/result');
        const { dataPoints } = getAssertionResultChartData(makeAssertion({ type: AssertionType.Volume }), [run]);

        expect(dataPoints[0].time).toBe(5000);
        expect(dataPoints[0].result.type).toBe(AssertionResultType.Failure);
        expect(dataPoints[0].result.resultUrl).toBe('https://example.com/result');
        expect(dataPoints[0].relatedRunEvent).toBe(run);
    });

    it('sets yValue to undefined (metric extraction not yet implemented)', () => {
        const run = makeRun(1000, AssertionResultType.Success);
        const { dataPoints } = getAssertionResultChartData(makeAssertion({ type: AssertionType.Volume }), [run]);
        expect(dataPoints[0].result.yValue).toBeUndefined();
    });
});

// ---------------------------------------------------------------------------
// getAssertionResultChartData — context
// ---------------------------------------------------------------------------

describe('getAssertionResultChartData — context', () => {
    it('passes the assertion through as context', () => {
        const assertion = makeAssertion({ type: AssertionType.Volume });
        const { context } = getAssertionResultChartData(assertion, []);
        expect(context.assertion).toBe(assertion);
    });
});

// ---------------------------------------------------------------------------
// getAssertionResultChartData — yAxisLabel
// ---------------------------------------------------------------------------

describe('getAssertionResultChartData — yAxisLabel', () => {
    it('returns "Row count" for Volume assertions', () => {
        const { yAxisLabel } = getAssertionResultChartData(makeAssertion({ type: AssertionType.Volume }), []);
        expect(yAxisLabel).toBe('Row count');
    });

    it('returns "SQL query result" for SQL assertions', () => {
        const { yAxisLabel } = getAssertionResultChartData(makeAssertion({ type: AssertionType.Sql }), []);
        expect(yAxisLabel).toBe('SQL query result');
    });

    it('returns "Freshness check results" for Freshness assertions', () => {
        const { yAxisLabel } = getAssertionResultChartData(makeAssertion({ type: AssertionType.Freshness }), []);
        expect(yAxisLabel).toBe('Freshness check results');
    });

    it('returns undefined for DataSchema assertions', () => {
        const { yAxisLabel } = getAssertionResultChartData(makeAssertion({ type: AssertionType.DataSchema }), []);
        expect(yAxisLabel).toBeUndefined();
    });

    it('returns undefined for Dataset assertions', () => {
        const { yAxisLabel } = getAssertionResultChartData(makeAssertion({ type: AssertionType.Dataset }), []);
        expect(yAxisLabel).toBeUndefined();
    });

    it('returns undefined when assertion has no info', () => {
        const { yAxisLabel } = getAssertionResultChartData(makeAssertion(undefined), []);
        expect(yAxisLabel).toBeUndefined();
    });

    it('returns "Invalid Rows" for Field / FieldValues assertions', () => {
        const { yAxisLabel } = getAssertionResultChartData(
            makeAssertion({
                type: AssertionType.Field,
                fieldAssertion: { type: FieldAssertionType.FieldValues } as any,
            }),
            [],
        );
        expect(yAxisLabel).toBe('Invalid Rows');
    });

    it('returns the readable metric label for Field / FieldMetric assertions with a known metric', () => {
        const { yAxisLabel } = getAssertionResultChartData(
            makeAssertion({
                type: AssertionType.Field,
                fieldAssertion: {
                    type: FieldAssertionType.FieldMetric,
                    fieldMetricAssertion: { metric: FieldMetricType.NullCount },
                } as any,
            }),
            [],
        );
        expect(yAxisLabel).toBe('Null count');
    });

    it('falls back to the raw metric value for Field / FieldMetric assertions with an unknown metric', () => {
        const { yAxisLabel } = getAssertionResultChartData(
            makeAssertion({
                type: AssertionType.Field,
                fieldAssertion: {
                    type: FieldAssertionType.FieldMetric,
                    fieldMetricAssertion: { metric: 'UNKNOWN_METRIC' as FieldMetricType },
                } as any,
            }),
            [],
        );
        expect(yAxisLabel).toBe('UNKNOWN_METRIC');
    });

    it('returns "Metric Value" for Field / FieldMetric assertions with no metric specified', () => {
        const { yAxisLabel } = getAssertionResultChartData(
            makeAssertion({
                type: AssertionType.Field,
                fieldAssertion: {
                    type: FieldAssertionType.FieldMetric,
                    fieldMetricAssertion: undefined,
                } as any,
            }),
            [],
        );
        expect(yAxisLabel).toBe('Metric Value');
    });
});
