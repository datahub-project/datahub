import { describe, expect, it } from 'vitest';

import {
    getAssertionGroupName,
    getAssertionType,
    getAssertionsSummary,
} from '@app/entityV2/shared/tabs/Dataset/Validations/acrylUtils';
import {
    Assertion,
    AssertionInfo,
    AssertionResultType,
    AssertionStdAggregation,
    AssertionStdOperator,
    AssertionType,
    CustomAssertionInfo,
    DatasetAssertionScope,
} from '@src/types.generated';

describe('getAssertionsSummary', () => {
    it('distinguishes initializing and never-run assertions', () => {
        const assertions = [
            {
                runEvents: {
                    runEvents: [{ result: { type: AssertionResultType.Init } }],
                },
            },
            {
                runEvents: {
                    runEvents: [{ result: { type: AssertionResultType.Success } }],
                },
            },
            { runEvents: { runEvents: [] } },
        ] as unknown as Assertion[];

        expect(getAssertionsSummary(assertions)).toEqual({
            passing: 1,
            failing: 0,
            erroring: 0,
            initializing: 1,
            notRunning: 1,
            total: 1,
            totalAssertions: 3,
        });
    });
});

const DATASET_URN = 'urn:li:dataset:(urn:li:dataPlatform:dbt,my_db.my_schema.events,PROD)';

const buildDbtAssertion = (customAssertion: Partial<CustomAssertionInfo>): Assertion =>
    ({
        urn: 'urn:li:assertion:1',
        info: {
            type: AssertionType.Custom,
            customAssertion: { type: 'dbt', entityUrn: DATASET_URN, ...customAssertion },
        } as AssertionInfo,
    }) as Assertion;

const getCategoryLabel = (assertion: Assertion) => getAssertionGroupName(getAssertionType(assertion) || '');

// The Category column must describe what an external-tool assertion checks, not which tool
// emitted it: every dbt test used to render as "DBT", indistinguishable from its siblings.
describe('getAssertionType for CUSTOM assertions', () => {
    it('categorizes a column-scoped dbt test as a Column assertion', () => {
        const notNull = buildDbtAssertion({
            scope: DatasetAssertionScope.DatasetColumn,
            operator: AssertionStdOperator.NotNull,
            aggregation: AssertionStdAggregation.Identity,
            nativeType: 'not_null_events_col_a',
            fields: [{ urn: `urn:li:schemaField:(${DATASET_URN},col_a)`, path: 'col_a' }],
        });

        expect(getAssertionType(notNull)).toBe(AssertionType.Field);
        expect(getCategoryLabel(notNull)).toBe('Column');
    });

    it('categorizes a column reference without a scope as a Column assertion', () => {
        // e.g. Snowflake DMFs and ODCS fallback rules set the column but never a scope
        const nullCount = buildDbtAssertion({
            type: 'Snowflake Data Metric Function',
            field: { urn: `urn:li:schemaField:(${DATASET_URN},col_a)`, path: 'col_a' },
        });

        expect(getAssertionType(nullCount)).toBe(AssertionType.Field);
        expect(getCategoryLabel(nullCount)).toBe('Column');
    });

    it('lets an explicit row scope win over a column reference', () => {
        const rowLevelWithField = buildDbtAssertion({
            scope: DatasetAssertionScope.DatasetRows,
            aggregation: AssertionStdAggregation.Native,
            fields: [{ urn: `urn:li:schemaField:(${DATASET_URN},col_a)`, path: 'col_a' }],
        });

        expect(getAssertionType(rowLevelWithField)).toBe(AssertionType.Custom);
    });

    it('categorizes a schema-scoped assertion as a Schema assertion', () => {
        const columnsMatchSet = buildDbtAssertion({
            type: 'greatExpectations',
            scope: DatasetAssertionScope.DatasetSchema,
            operator: AssertionStdOperator.EqualTo,
            aggregation: AssertionStdAggregation.Columns,
            nativeType: 'expect_table_columns_to_match_set',
        });

        expect(getAssertionType(columnsMatchSet)).toBe(AssertionType.DataSchema);
        expect(getCategoryLabel(columnsMatchSet)).toBe('Schema');
    });

    it('categorizes a row-count assertion as a Volume assertion', () => {
        const rowCount = buildDbtAssertion({
            scope: DatasetAssertionScope.DatasetRows,
            operator: AssertionStdOperator.GreaterThanOrEqualTo,
            aggregation: AssertionStdAggregation.RowCount,
            nativeType: 'expect_table_row_count_to_be_between_events',
        });

        expect(getAssertionType(rowCount)).toBe(AssertionType.Volume);
        expect(getCategoryLabel(rowCount)).toBe('Volume');
    });

    it('falls back to Custom for a dbt test without a recognizable shape', () => {
        const singularTest = buildDbtAssertion({
            scope: DatasetAssertionScope.DatasetRows,
            operator: AssertionStdOperator.Native,
            aggregation: AssertionStdAggregation.Native,
            nativeType: 'assert_events_are_consistent',
        });
        const noStructuredFields = buildDbtAssertion({});

        expect(getAssertionType(singularTest)).toBe(AssertionType.Custom);
        expect(getAssertionType(noStructuredFields)).toBe(AssertionType.Custom);
        expect(getCategoryLabel(noStructuredFields)).toBe('Custom');
    });

    it('leaves native assertion types unchanged', () => {
        const volume = { urn: 'urn:li:assertion:2', info: { type: AssertionType.Volume } } as Assertion;

        expect(getAssertionType(volume)).toBe(AssertionType.Volume);
    });
});
