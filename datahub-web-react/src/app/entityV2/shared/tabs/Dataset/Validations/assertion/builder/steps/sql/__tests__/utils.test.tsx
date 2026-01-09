import { describe, expect, it } from 'vitest';

import {
    SQL_OPERATION_OPTIONS,
    SqlOperationOptionEnum,
    getOperationOption,
    getSqlOperationOptionGroups,
    getSqlOperationOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/sql/utils';
import { SqlAssertionBuilderOperatorOptions } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';

import { SqlAssertionType } from '@types';

describe('SQL Assertion Builder Utils', () => {
    describe('SQL_OPERATION_OPTIONS', () => {
        it('should map AI_INFERRED to AiInferred operator', () => {
            const aiOption = SQL_OPERATION_OPTIONS[SqlOperationOptionEnum.AI_INFERRED];
            expect(aiOption.operator).toBe(SqlAssertionBuilderOperatorOptions.AiInferred);
            expect(aiOption.type).toBe(SqlAssertionType.Metric);
        });

        it('should map IS_BETWEEN to Between operator', () => {
            const rangeOption = SQL_OPERATION_OPTIONS[SqlOperationOptionEnum.IS_BETWEEN];
            expect(rangeOption.operator).toBe(SqlAssertionBuilderOperatorOptions.Between);
            expect(rangeOption.type).toBe(SqlAssertionType.Metric);
        });
    });

    describe('getOperationOption', () => {
        it('should return AI_INFERRED when operator is AiInferred', () => {
            const result = getOperationOption(SqlAssertionType.Metric, SqlAssertionBuilderOperatorOptions.AiInferred);
            expect(result).toBe(SqlOperationOptionEnum.AI_INFERRED);
        });

        it('should return IS_BETWEEN when type is Metric and operator is Between', () => {
            const result = getOperationOption(SqlAssertionType.Metric, SqlAssertionBuilderOperatorOptions.Between);
            expect(result).toBe(SqlOperationOptionEnum.IS_BETWEEN);
        });

        it('should distinguish between AI_INFERRED and IS_BETWEEN', () => {
            const aiResult = getOperationOption(SqlAssertionType.Metric, SqlAssertionBuilderOperatorOptions.AiInferred);
            const rangeResult = getOperationOption(SqlAssertionType.Metric, SqlAssertionBuilderOperatorOptions.Between);

            expect(aiResult).not.toBe(rangeResult);
            expect(aiResult).toBe(SqlOperationOptionEnum.AI_INFERRED);
            expect(rangeResult).toBe(SqlOperationOptionEnum.IS_BETWEEN);
        });

        it('should return undefined for invalid type/operator combinations', () => {
            const result = getOperationOption(SqlAssertionType.Metric, null as any);
            expect(result).toBeUndefined();
        });
    });

    describe('getSqlOperationOptionGroups', () => {
        it('should group AI_INFERRED separately from other metrics', () => {
            const groups = getSqlOperationOptionGroups();

            // Check structure of groups [label, options]
            const aiGroup = groups.find((g) => g.label === 'Anomaly Detection');
            expect(aiGroup).toBeDefined();
            expect(aiGroup?.options).toHaveLength(1);
            expect(aiGroup?.options[0].value).toBe(SqlOperationOptionEnum.AI_INFERRED);

            const metricGroup = groups.find((g) => g.label === 'Absolute Value');
            expect(metricGroup).toBeDefined();
            // Should contain IS_EQUAL_TO, IS_NOT_EQUAL_TO, IS_GREATER_THAN, IS_LESS_THAN, IS_BETWEEN
            // But NOT AI_INFERRED
            expect(metricGroup?.options.find((o) => o.value === SqlOperationOptionEnum.AI_INFERRED)).toBeUndefined();
            expect(metricGroup?.options.find((o) => o.value === SqlOperationOptionEnum.IS_BETWEEN)).toBeDefined();
        });
    });

    describe('getSqlOperationOptions', () => {
        it('should return all options', () => {
            const options = getSqlOperationOptions();
            expect(options.length).toBe(Object.keys(SQL_OPERATION_OPTIONS).length);
        });
    });
});
