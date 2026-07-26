import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import { DatasetAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/DatasetAssertionDescription';
import { FieldAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/FieldAssertionDescription';
import { FreshnessAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/FreshnessAssertionDescription';
import { SchemaAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/SchemaAssertionDescription';
import { VolumeAssertionDescription } from '@app/entityV2/shared/tabs/Dataset/Validations/VolumeAssertionDescription';
import { getPlainTextDescriptionFromAssertion } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/utils';
import { FreshnessScheduleSummary } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/FreshnessScheduleSummary';
import { getFieldDescriptionDescriptor } from '@app/entityV2/shared/tabs/Dataset/Validations/fieldDescriptionUtils';
import themeV2 from '@conf/theme/themeV2';
import enValidations from '@src/i18n/locales/en/entity.profile.validations.json';

import {
    AssertionInfo,
    AssertionStdAggregation,
    AssertionStdOperator,
    AssertionStdParameterType,
    AssertionType,
    AssertionValueChangeType,
    DatasetAssertionScope,
    FieldAssertionType,
    FieldMetricType,
    FieldTransformType,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
    SchemaAssertionCompatibility,
    VolumeAssertionType,
} from '@types';

/**
 * Verifies the exact rendered English text of every assertion-description branch (and the plaintext
 * search path). Assertions use container.textContent, which is robust to JSX/markup restructuring and
 * only changes when visible text changes — so these double as regression protection for the
 * description translation keys.
 */

const renderText = (node: React.ReactElement): string =>
    render(
        <MockedProvider>
            <ThemeProvider theme={themeV2}>{node}</ThemeProvider>
        </MockedProvider>,
    ).container.textContent ?? '';

const numberParam = (value: string | number) => ({ value: String(value), type: AssertionStdParameterType.Number });

describe('DatasetAssertionDescription', () => {
    // Each operator rendered against a fixed (column / UniqueCount) aggregation prefix.
    const operators: Array<[string, AssertionStdOperator, any, string]> = [
        [
            'between',
            AssertionStdOperator.Between,
            { minValue: numberParam(5), maxValue: numberParam(10) },
            'Unique value count for column profileId is between 5 and 10',
        ],
        [
            'equalTo',
            AssertionStdOperator.EqualTo,
            { value: numberParam(5) },
            'Unique value count for column profileId is equal to 5',
        ],
        [
            'contain',
            AssertionStdOperator.Contain,
            { value: numberParam(5) },
            'Unique value count for column profileId contains 5',
        ],
        ['in', AssertionStdOperator.In, { value: numberParam(5) }, 'Unique value count for column profileId is in 5'],
        ['notNull', AssertionStdOperator.NotNull, {}, 'Unique value count for column profileId is not null'],
        [
            'greaterThan',
            AssertionStdOperator.GreaterThan,
            { value: numberParam(5) },
            'Unique value count for column profileId is greater than 5',
        ],
        [
            'greaterThanOrEqualTo',
            AssertionStdOperator.GreaterThanOrEqualTo,
            { value: numberParam(5) },
            'Unique value count for column profileId is greater than or equal to 5',
        ],
        [
            'lessThan',
            AssertionStdOperator.LessThan,
            { value: numberParam(5) },
            'Unique value count for column profileId is less than 5',
        ],
        [
            'lessThanOrEqualTo',
            AssertionStdOperator.LessThanOrEqualTo,
            { value: numberParam(5) },
            'Unique value count for column profileId is less than or equal to 5',
        ],
        [
            'startWith',
            AssertionStdOperator.StartWith,
            { value: numberParam(5) },
            'Unique value count for column profileId starts with 5',
        ],
        [
            'endWith',
            AssertionStdOperator.EndWith,
            { value: numberParam(5) },
            'Unique value count for column profileId ends with 5',
        ],
        [
            'native',
            AssertionStdOperator.Native,
            {},
            'Unique value count for column profileId is passing assertion MY_NATIVE',
        ],
        [
            'default(null)',
            AssertionStdOperator.Null,
            { value: numberParam(5) },
            'Unique value count for column profileId is passing operator NULL with value $5',
        ],
    ];

    it.each(operators)('operator %s', (_name, operator, parameters, expected) => {
        const info = {
            scope: DatasetAssertionScope.DatasetColumn,
            aggregation: AssertionStdAggregation.UniqueCount,
            fields: [{ path: 'profileId' }],
            operator,
            parameters,
            nativeType: 'MY_NATIVE',
        } as any;
        expect(renderText(<DatasetAssertionDescription assertionInfo={info} />)).toBe(expected);
    });

    // Each aggregation rendered against a fixed (greater than 5) operator suffix.
    const aggregations: Array<[string, DatasetAssertionScope, AssertionStdAggregation, string]> = [
        [
            'schemaColumnCount',
            DatasetAssertionScope.DatasetSchema,
            AssertionStdAggregation.ColumnCount,
            'Dataset column count is greater than 5',
        ],
        [
            'schemaColumns',
            DatasetAssertionScope.DatasetSchema,
            AssertionStdAggregation.Columns,
            'Dataset columns are greater than 5',
        ],
        [
            'schemaNative',
            DatasetAssertionScope.DatasetSchema,
            AssertionStdAggregation.Native,
            'Dataset columns ["profileId"] are greater than 5',
        ],
        [
            'rowsRowCount',
            DatasetAssertionScope.DatasetRows,
            AssertionStdAggregation.RowCount,
            'Dataset row count is greater than 5',
        ],
        [
            'rowsNative',
            DatasetAssertionScope.DatasetRows,
            AssertionStdAggregation.Native,
            'Dataset rows are greater than 5',
        ],
        [
            'colUniqueCount',
            DatasetAssertionScope.DatasetColumn,
            AssertionStdAggregation.UniqueCount,
            'Unique value count for column profileId is greater than 5',
        ],
        [
            'colUniqueProportion',
            DatasetAssertionScope.DatasetColumn,
            AssertionStdAggregation.UniquePropotion,
            'Unique value proportion for column profileId is greater than 5',
        ],
        [
            'colNullCount',
            DatasetAssertionScope.DatasetColumn,
            AssertionStdAggregation.NullCount,
            'Null count for column profileId is greater than 5',
        ],
        [
            'colNullProportion',
            DatasetAssertionScope.DatasetColumn,
            AssertionStdAggregation.NullProportion,
            'Null proportion for column profileId is greater than 5',
        ],
        [
            'colMin',
            DatasetAssertionScope.DatasetColumn,
            AssertionStdAggregation.Min,
            'Minimum value for column profileId is greater than 5',
        ],
        [
            'colMax',
            DatasetAssertionScope.DatasetColumn,
            AssertionStdAggregation.Max,
            'Maximum value for column profileId is greater than 5',
        ],
        [
            'colMean',
            DatasetAssertionScope.DatasetColumn,
            AssertionStdAggregation.Mean,
            'Mean value for column profileId is greater than 5',
        ],
        [
            'colMedian',
            DatasetAssertionScope.DatasetColumn,
            AssertionStdAggregation.Median,
            'Median value for column profileId is greater than 5',
        ],
        [
            'colStddev',
            DatasetAssertionScope.DatasetColumn,
            AssertionStdAggregation.Stddev,
            'Standard deviation for column profileId is greater than 5',
        ],
        [
            'colNative',
            DatasetAssertionScope.DatasetColumn,
            AssertionStdAggregation.Native,
            'Column profileId values are greater than 5',
        ],
        [
            'colDefault',
            DatasetAssertionScope.DatasetColumn,
            AssertionStdAggregation.ColumnCount,
            'Column profileId values are greater than 5',
        ],
    ];

    it.each(aggregations)('aggregation %s', (_name, scope, aggregation, expected) => {
        const info = {
            scope,
            aggregation,
            fields: [{ path: 'profileId' }],
            operator: AssertionStdOperator.GreaterThan,
            parameters: { value: numberParam(5) },
        } as any;
        expect(renderText(<DatasetAssertionDescription assertionInfo={info} />)).toBe(expected);
    });

    it('renders an explicit description verbatim', () => {
        expect(
            renderText(<DatasetAssertionDescription description="My custom description" assertionInfo={{} as any} />),
        ).toBe('My custom description');
    });
});

describe('VolumeAssertionDescription', () => {
    const cases: Array<[string, any, string]> = [
        [
            'rowCountTotal + atLeast',
            {
                type: VolumeAssertionType.RowCountTotal,
                rowCountTotal: {
                    operator: AssertionStdOperator.GreaterThanOrEqualTo,
                    parameters: { value: numberParam(100) },
                },
            },
            'Table has at least 100 rows',
        ],
        [
            'rowCountTotal + atMost',
            {
                type: VolumeAssertionType.RowCountTotal,
                rowCountTotal: {
                    operator: AssertionStdOperator.LessThanOrEqualTo,
                    parameters: { value: numberParam(100) },
                },
            },
            'Table has at most 100 rows',
        ],
        [
            'rowCountTotal + between',
            {
                type: VolumeAssertionType.RowCountTotal,
                rowCountTotal: {
                    operator: AssertionStdOperator.Between,
                    parameters: { minValue: numberParam(5), maxValue: numberParam(10) },
                },
            },
            'Table has between 5 and 10 rows',
        ],
        [
            'rowCountChange absolute + atLeast',
            {
                type: VolumeAssertionType.RowCountChange,
                rowCountChange: {
                    type: AssertionValueChangeType.Absolute,
                    operator: AssertionStdOperator.GreaterThanOrEqualTo,
                    parameters: { value: numberParam(100) },
                },
            },
            'Table should grow by at least 100 rows',
        ],
        [
            'rowCountChange percentage + atMost',
            {
                type: VolumeAssertionType.RowCountChange,
                rowCountChange: {
                    type: AssertionValueChangeType.Percentage,
                    operator: AssertionStdOperator.LessThanOrEqualTo,
                    parameters: { value: numberParam(50) },
                },
            },
            'Table should grow by at most 50 %',
        ],
        [
            'incrementingSegmentRowCountTotal + atLeast',
            {
                type: VolumeAssertionType.IncrementingSegmentRowCountTotal,
                incrementingSegmentRowCountTotal: {
                    operator: AssertionStdOperator.GreaterThanOrEqualTo,
                    parameters: { value: numberParam(100) },
                },
            },
            'Table has at least 100 rows',
        ],
        [
            'incrementingSegmentRowCountChange percentage + between',
            {
                type: VolumeAssertionType.IncrementingSegmentRowCountChange,
                incrementingSegmentRowCountChange: {
                    type: AssertionValueChangeType.Percentage,
                    operator: AssertionStdOperator.Between,
                    parameters: { minValue: numberParam(5), maxValue: numberParam(10) },
                },
            },
            'Table should grow by between 5 and 10 %',
        ],
        [
            // Volume assertions only ever use GreaterThanOrEqualTo/LessThanOrEqualTo/Between, but the
            // operator type allows any AssertionStdOperator. An out-of-range operator must degrade to
            // a generic description rather than throw and crash the Validation tab.
            'unexpected operator falls back',
            {
                type: VolumeAssertionType.RowCountTotal,
                rowCountTotal: {
                    operator: AssertionStdOperator.EqualTo,
                    parameters: { value: numberParam(100) },
                },
            },
            'Table has a volume assertion',
        ],
    ];

    it.each(cases)('%s', (_name, info, expected) => {
        expect(renderText(<VolumeAssertionDescription assertionInfo={info as any} />)).toBe(expected);
    });
});

describe('SchemaAssertionDescription', () => {
    const cases: Array<[string, SchemaAssertionCompatibility, number, string]> = [
        [
            'exactMatch single',
            SchemaAssertionCompatibility.ExactMatch,
            1,
            'Actual table columns exactly match 1 expected column',
        ],
        [
            'exactMatch many',
            SchemaAssertionCompatibility.ExactMatch,
            3,
            'Actual table columns exactly match 3 expected columns',
        ],
        [
            'subset (include) single',
            SchemaAssertionCompatibility.Subset,
            1,
            'Actual table columns include 1 expected column',
        ],
        [
            'subset (include) many',
            SchemaAssertionCompatibility.Subset,
            3,
            'Actual table columns include 3 expected columns',
        ],
    ];

    it.each(cases)('%s', (_name, compatibility, count, expected) => {
        const info = {
            compatibility,
            fields: Array.from({ length: count }, (_, i) => ({ path: `col_${i}` })),
        } as any;
        expect(renderText(<SchemaAssertionDescription assertionInfo={info} />)).toBe(expected);
    });
});

describe('FreshnessAssertionDescription', () => {
    const cron = { cron: '0 0 * * *', timezone: 'UTC' } as any;

    const cases: Array<[string, FreshnessAssertionType, any, string]> = [
        [
            'datasetChange fixedInterval',
            FreshnessAssertionType.DatasetChange,
            { type: FreshnessAssertionScheduleType.FixedInterval, fixedInterval: { multiple: 5, unit: 'HOUR' } },
            'Table was updated in the past 5 hours',
        ],
        [
            'datasetChange cron',
            FreshnessAssertionType.DatasetChange,
            { type: FreshnessAssertionScheduleType.Cron, cron },
            'Table was updated between cron windows scheduled at 12:00 am (UTC)',
        ],
        [
            'datasetChange sinceLastCheck',
            FreshnessAssertionType.DatasetChange,
            { type: FreshnessAssertionScheduleType.SinceTheLastCheck },
            'Table was updated since the previous check.',
        ],
        [
            'dataJobRun fixedInterval',
            FreshnessAssertionType.DataJobRun,
            { type: FreshnessAssertionScheduleType.FixedInterval, fixedInterval: { multiple: 1, unit: 'DAY' } },
            'Data Task is run successfully in the past 1 days',
        ],
        [
            'dataJobRun cron',
            FreshnessAssertionType.DataJobRun,
            { type: FreshnessAssertionScheduleType.Cron, cron },
            'Data Task is run successfully between cron windows scheduled at 12:00 am (UTC)',
        ],
    ];

    it.each(cases)('%s', (_name, type, schedule, expected) => {
        const info = { type, schedule } as any;
        expect(renderText(<FreshnessAssertionDescription assertionInfo={info} />)).toBe(expected);
    });
});

describe('FreshnessScheduleSummary', () => {
    const cron = { cron: '0 0 * * *', timezone: 'UTC' } as any;

    it('cron', () => {
        const definition = { type: FreshnessAssertionScheduleType.Cron, cron } as any;
        expect(renderText(<FreshnessScheduleSummary definition={definition} />)).toBe('At 12:00 am.');
    });
    it('sinceLastCheck with cron', () => {
        const definition = { type: FreshnessAssertionScheduleType.SinceTheLastCheck } as any;
        expect(renderText(<FreshnessScheduleSummary definition={definition} evaluationSchedule={cron} />)).toBe(
            'Since the previous check, as of at 12:00 am',
        );
    });
    it('fixedInterval no cron', () => {
        const definition = {
            type: FreshnessAssertionScheduleType.FixedInterval,
            fixedInterval: { multiple: 5, unit: 'HOUR' },
        } as any;
        expect(renderText(<FreshnessScheduleSummary definition={definition} />)).toBe('In the past 5 hours');
    });
    it('fixedInterval with cron', () => {
        const definition = {
            type: FreshnessAssertionScheduleType.FixedInterval,
            fixedInterval: { multiple: 5, unit: 'HOUR' },
        } as any;
        expect(renderText(<FreshnessScheduleSummary definition={definition} evaluationSchedule={cron} />)).toBe(
            'In the past 5 hours, as of at 12:00 am',
        );
    });
});

describe('FieldAssertionDescription', () => {
    it('field values, column tag hidden', () => {
        const info = {
            type: FieldAssertionType.FieldValues,
            fieldValuesAssertion: {
                field: { path: 'profileId' },
                operator: AssertionStdOperator.GreaterThan,
                parameters: { value: numberParam(5) },
            },
        } as any;
        expect(renderText(<FieldAssertionDescription assertionInfo={info} />)).toBe('profileId is greater than 5');
    });
    it('field values, column tag shown', () => {
        const info = {
            type: FieldAssertionType.FieldValues,
            fieldValuesAssertion: {
                field: { path: 'profileId' },
                operator: AssertionStdOperator.GreaterThan,
                parameters: { value: numberParam(5) },
            },
        } as any;
        expect(renderText(<FieldAssertionDescription assertionInfo={info} showColumnTag />)).toBe(
            'Values are greater than 5profileId',
        );
    });
    it('field metric with transform', () => {
        const info = {
            type: FieldAssertionType.FieldMetric,
            fieldMetricAssertion: {
                field: { path: 'profileId' },
                metric: FieldMetricType.NullCount,
                operator: AssertionStdOperator.LessThan,
                parameters: { value: numberParam(5) },
            },
        } as any;
        expect(renderText(<FieldAssertionDescription assertionInfo={info} showColumnTag />)).toBe(
            'Null count of column is less than 5profileId',
        );
    });
    it('field values transform length', () => {
        const info = {
            type: FieldAssertionType.FieldValues,
            fieldValuesAssertion: {
                field: { path: 'name' },
                transform: { type: FieldTransformType.Length },
                operator: AssertionStdOperator.GreaterThan,
                parameters: { value: numberParam(5) },
            },
        } as any;
        expect(renderText(<FieldAssertionDescription assertionInfo={info} showColumnTag />)).toBe(
            'Length of column is greater than 5name',
        );
    });
    it('explicit description', () => {
        const info = {
            type: FieldAssertionType.FieldValues,
            fieldValuesAssertion: { field: { path: 'profileId' } },
        } as any;
        expect(
            renderText(<FieldAssertionDescription assertionInfo={info} assertionDescription="My field description" />),
        ).toBe('My field description');
    });
    it('field metric, column tag hidden', () => {
        const info = {
            type: FieldAssertionType.FieldMetric,
            fieldMetricAssertion: {
                field: { path: 'profileId' },
                metric: FieldMetricType.NullCount,
                operator: AssertionStdOperator.LessThan,
                parameters: { value: numberParam(5) },
            },
        } as any;
        expect(renderText(<FieldAssertionDescription assertionInfo={info} />)).toBe(
            'Null count of profileId is less than 5',
        );
    });
    it('field values, between', () => {
        const info = {
            type: FieldAssertionType.FieldValues,
            fieldValuesAssertion: {
                field: { path: 'profileId' },
                operator: AssertionStdOperator.Between,
                parameters: { minValue: numberParam(5), maxValue: numberParam(10) },
            },
        } as any;
        expect(renderText(<FieldAssertionDescription assertionInfo={info} />)).toBe(
            'profileId is within a range 5 and 10',
        );
    });
    it('field values, no-value operator (not null)', () => {
        const info = {
            type: FieldAssertionType.FieldValues,
            fieldValuesAssertion: {
                field: { path: 'profileId' },
                operator: AssertionStdOperator.NotNull,
            },
        } as any;
        expect(renderText(<FieldAssertionDescription assertionInfo={info} />)).toBe('profileId is not null');
    });
    it('unsupported operator falls back', () => {
        const info = {
            type: FieldAssertionType.FieldValues,
            fieldValuesAssertion: {
                field: { path: 'myField' },
                operator: AssertionStdOperator.Native,
            },
        } as any;
        expect(renderText(<FieldAssertionDescription assertionInfo={info} />)).toContain('myField');
    });
});

describe('getPlainTextDescriptionFromAssertion (search path)', () => {
    it('explicit description wins', () => {
        expect(getPlainTextDescriptionFromAssertion({ description: 'explicit' } as AssertionInfo)).toBe('explicit');
    });
    it('dataset', () => {
        const info = {
            type: AssertionType.Dataset,
            datasetAssertion: {
                scope: DatasetAssertionScope.DatasetColumn,
                aggregation: AssertionStdAggregation.UniqueCount,
                fields: [{ path: 'profileId' }],
                operator: AssertionStdOperator.GreaterThan,
                parameters: { value: numberParam(5) },
            },
        } as any;
        expect(getPlainTextDescriptionFromAssertion(info)).toBe(
            'Unique value count for column profileId is greater than 5',
        );
    });
    it('volume', () => {
        const info = {
            type: AssertionType.Volume,
            volumeAssertion: {
                type: VolumeAssertionType.RowCountTotal,
                rowCountTotal: {
                    operator: AssertionStdOperator.GreaterThanOrEqualTo,
                    parameters: { value: numberParam(100) },
                },
            },
        } as any;
        expect(getPlainTextDescriptionFromAssertion(info)).toBe('Table has at least 100 rows');
    });
    it('schema', () => {
        const info = {
            type: AssertionType.DataSchema,
            schemaAssertion: {
                compatibility: SchemaAssertionCompatibility.ExactMatch,
                fields: [{ path: 'a' }, { path: 'b' }],
            },
        } as any;
        expect(getPlainTextDescriptionFromAssertion(info)).toBe(
            'Actual table columns exactly match 2 expected columns',
        );
    });
    it('field', () => {
        const info = {
            type: AssertionType.Field,
            fieldAssertion: {
                type: FieldAssertionType.FieldValues,
                fieldValuesAssertion: {
                    field: { path: 'profileId' },
                    operator: AssertionStdOperator.GreaterThan,
                    parameters: { value: numberParam(5) },
                },
            },
        } as any;
        expect(getPlainTextDescriptionFromAssertion(info)).toBe('profileId is greater than 5');
    });
    it('field falls back to a generic description on unsupported operator', () => {
        const info = {
            type: AssertionType.Field,
            fieldAssertion: {
                type: FieldAssertionType.FieldValues,
                fieldValuesAssertion: {
                    field: { path: 'profileId' },
                    operator: AssertionStdOperator.Native,
                    parameters: { value: numberParam(5) },
                },
            },
        } as any;
        expect(getPlainTextDescriptionFromAssertion(info)).toBe('Custom check on profileId');
    });
    it('freshness', () => {
        const info = {
            type: AssertionType.Freshness,
            freshnessAssertion: {
                type: FreshnessAssertionType.DatasetChange,
                schedule: {
                    type: FreshnessAssertionScheduleType.FixedInterval,
                    fixedInterval: { multiple: 5, unit: 'HOUR' },
                },
            },
        } as any;
        expect(getPlainTextDescriptionFromAssertion(info)).toBe('Table was updated in the past 5 hours');
    });
});

/**
 * The Dataset/Volume description keys are composed dynamically (`datasetDescription.<agg>.<op>`,
 * `volumeDescription.<...>`). Dynamic keys are invisible to static extraction, so these guard that every
 * combination the component can build actually has a translation key — independent of (and complementary
 * to) the compile-time check once i18next typed resources are re-enabled.
 */
describe('description key completeness', () => {
    const AGGREGATION_KEYS = [
        'columnCount',
        'columns',
        'schemaNativeColumns',
        'rowCount',
        'rows',
        'uniqueCount',
        'uniqueProportion',
        'nullCount',
        'nullProportion',
        'min',
        'max',
        'mean',
        'median',
        'stddev',
        'columnValues',
        'dataset',
    ];
    const OPERATOR_KEYS = [
        'between',
        'equalTo',
        'contains',
        'in',
        'notNull',
        'greaterThan',
        'greaterThanOrEqualTo',
        'lessThan',
        'lessThanOrEqualTo',
        'startsWith',
        'endsWith',
        'native',
        'fallback',
    ];
    const VOLUME_KEYS = [
        'totalAtLeast',
        'totalAtMost',
        'totalBetween',
        'changeAtLeastRows',
        'changeAtMostRows',
        'changeBetweenRows',
        'changeAtLeastPercent',
        'changeAtMostPercent',
        'changeBetweenPercent',
        'unknown',
    ];

    it('has a key for every Dataset aggregation × operator combination', () => {
        const missing = AGGREGATION_KEYS.flatMap((agg) =>
            OPERATOR_KEYS.map((op) => `datasetDescription.${agg}.${op}`).filter((key) => !(key in enValidations)),
        );
        expect(missing).toEqual([]);
    });

    it('has a key for every Volume description variant', () => {
        const missing = VOLUME_KEYS.map((k) => `volumeDescription.${k}`).filter((key) => !(key in enValidations));
        expect(missing).toEqual([]);
    });
});

describe('field description key completeness', () => {
    const FIELD_SUBJECT_SHAPES = [
        'values',
        'valuesTransform',
        'metric',
        'valuesColumn',
        'valuesTransformColumn',
        'metricColumn',
    ];
    const FIELD_OPERATOR_KEYS = [
        'between',
        'equalTo',
        'notEqualTo',
        'contains',
        'regexMatch',
        'in',
        'notIn',
        'null',
        'notNull',
        'isTrue',
        'isFalse',
        'greaterThan',
        'greaterThanOrEqualTo',
        'lessThan',
        'lessThanOrEqualTo',
    ];
    const FIELD_METRIC_LABEL_KEYS = [
        'nullCount',
        'nullPercentage',
        'uniqueCount',
        'uniquePercentage',
        'maxLength',
        'minLength',
        'emptyCount',
        'emptyPercentage',
        'max',
        'min',
        'mean',
        'median',
        'negativeCount',
        'negativePercentage',
        'stddev',
        'zeroCount',
        'zeroPercentage',
    ];

    it('has a key for every field subject shape × operator combination', () => {
        const missing = FIELD_SUBJECT_SHAPES.flatMap((shape) =>
            FIELD_OPERATOR_KEYS.map((op) => `fieldDescription.${shape}.${op}`).filter((key) => !(key in enValidations)),
        );
        expect(missing).toEqual([]);
    });

    it('has a label key for every field metric type and transform', () => {
        const missing = [
            ...FIELD_METRIC_LABEL_KEYS.map((m) => `fieldDescription.metricLabel.${m}`),
            'fieldDescription.transformLabel.length',
        ].filter((key) => !(key in enValidations));
        expect(missing).toEqual([]);
    });
});

describe('getFieldDescriptionDescriptor', () => {
    it('field values, no transform → values shape', () => {
        const info = {
            type: FieldAssertionType.FieldValues,
            fieldValuesAssertion: {
                field: { path: 'profileId' },
                operator: AssertionStdOperator.GreaterThan,
                parameters: { value: numberParam(5) },
            },
        } as any;
        const d = getFieldDescriptionDescriptor(info);
        expect(d.shape).toBe('values');
        expect(d.operatorKey).toBe('greaterThan');
        expect(d.field).toBe('profileId');
        expect(d.tokens.value).toBe('5');
    });

    it('field metric, showColumnTag → metricColumn shape with metric label key', () => {
        const info = {
            type: FieldAssertionType.FieldMetric,
            fieldMetricAssertion: {
                field: { path: 'profileId' },
                metric: FieldMetricType.NullCount,
                operator: AssertionStdOperator.LessThan,
                parameters: { value: numberParam(5) },
            },
        } as any;
        const d = getFieldDescriptionDescriptor(info, { showColumnTag: true });
        expect(d.shape).toBe('metricColumn');
        expect(d.metricLabelKey).toBe('fieldDescription.metricLabel.nullCount');
    });

    it('between → min/max tokens', () => {
        const info = {
            type: FieldAssertionType.FieldValues,
            fieldValuesAssertion: {
                field: { path: 'profileId' },
                operator: AssertionStdOperator.Between,
                parameters: { minValue: numberParam(5), maxValue: numberParam(10) },
            },
        } as any;
        const d = getFieldDescriptionDescriptor(info);
        expect(d.operatorKey).toBe('between');
        expect(d.tokens.minValue).toBe('5');
        expect(d.tokens.maxValue).toBe('10');
    });

    it('unsupported operator throws (→ component fallback)', () => {
        const info = {
            type: FieldAssertionType.FieldValues,
            fieldValuesAssertion: {
                field: { path: 'x' },
                operator: AssertionStdOperator.Native,
            },
        } as any;
        expect(() => getFieldDescriptionDescriptor(info)).toThrow();
    });
});
