import { describe, expect, it } from 'vitest';

import {
    FieldMetricAssertionBuilderOperatorOptions,
    SqlAssertionBuilderOperatorOptions,
    VolumeAssertionBuilderTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import {
    builderStateToSharedFieldAssertionVariables,
    builderStateToSharedSqlAssertionVariables,
    builderStateToSharedVolumeAssertionVariables,
    createAssertionMonitorBuilderState,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/utils';

import { Assertion } from '@types';

describe('Assertion Builder State Conversion', () => {
    // Mock Entity and Monitor
    const mockEntity = { urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,sample,PROD)' };
    const mockMonitor = {
        info: {
            assertionMonitor: {
                assertions: [{ schedule: { cron: '* * * * *', timezone: 'UTC' } }],
            },
        },
    } as any;

    describe('SQL Assertion Round-Trip', () => {
        it('should correctly handle AI Inferred SQL Assertion (Loading -> Saving)', () => {
            // 1. Simulate Loading from GMS (Source = Inferred)
            const aiAssertion: Assertion = {
                urn: 'urn:li:assertion:123',
                info: {
                    type: 'SQL' as any, // AssertionType.Sql
                    sqlAssertion: {
                        type: 'METRIC' as any, // SqlAssertionType.Metric
                        operator: 'BETWEEN' as any, // AssertionStdOperator.Between
                        statement: 'SELECT count(*) FROM table',
                    },
                    source: {
                        type: 'INFERRED' as any, // AssertionSourceType.Inferred
                    },
                },
            } as any;

            const builderState = createAssertionMonitorBuilderState(aiAssertion, mockEntity, mockMonitor);

            // Verify Loader mapped it to the extended operator
            expect(builderState.assertion?.sqlAssertion?.operator).toBe(SqlAssertionBuilderOperatorOptions.AiInferred);

            // 2. Simulate Saving to GMS
            const variables = builderStateToSharedSqlAssertionVariables(builderState);

            // Verify Saver maps it back to Standard Operator + inferWithAI flag (implicit in logic usage, but here checking exact output)
            expect(variables.operator).toBe('BETWEEN');
        });

        it('should correctly handle Range SQL Assertion (Loading -> Saving)', () => {
            // 1. Simulate Loading from GMS (Source != Inferred)
            const rangeAssertion: Assertion = {
                urn: 'urn:li:assertion:123',
                info: {
                    type: 'SQL' as any,
                    sqlAssertion: {
                        type: 'METRIC' as any,
                        operator: 'BETWEEN' as any,
                        statement: 'SELECT count(*) FROM table',
                        parameters: {
                            minValue: { value: '10', type: 'NUMBER' },
                            maxValue: { value: '100', type: 'NUMBER' },
                        },
                    },
                    source: {
                        type: 'USER' as any,
                    },
                },
            } as any;

            const builderState = createAssertionMonitorBuilderState(rangeAssertion, mockEntity, mockMonitor);

            // Verify Loader mapped it to the standard operator
            expect(builderState.assertion?.sqlAssertion?.operator).toBe('BETWEEN');

            // 2. Simulate Saving to GMS
            const variables = builderStateToSharedSqlAssertionVariables(builderState);

            expect(variables.operator).toBe('BETWEEN');
            expect(variables.parameters?.minValue?.value).toBe('10');
            expect(variables.parameters?.maxValue?.value).toBe('100');
        });
    });

    describe('Volume Assertion Round-Trip', () => {
        it('should correctly handle AI Inferred Volume Assertion', () => {
            // 1. Loading
            const aiAssertion: Assertion = {
                urn: 'urn:li:assertion:vol-ai',
                info: {
                    type: 'VOLUME' as any,
                    volumeAssertion: {
                        type: 'ROW_COUNT_TOTAL' as any,
                        rowCountTotal: { operator: 'BETWEEN' as any },
                    },
                    source: { type: 'INFERRED' as any },
                },
            } as any;

            const builderState = createAssertionMonitorBuilderState(aiAssertion, mockEntity, mockMonitor);

            // Should map to extended type
            expect(builderState.assertion?.volumeAssertion?.type).toBe(
                VolumeAssertionBuilderTypeOptions.AiInferredRowCountTotal,
            );

            // 2. Saving
            const variables = builderStateToSharedVolumeAssertionVariables(builderState);

            // Should map back to standard type
            expect(variables.type).toBe('ROW_COUNT_TOTAL');
            expect((variables as any).rowCountTotal?.operator).toBe('BETWEEN');
        });

        it('should correctly handle Standard Volume Assertion', () => {
            // 1. Loading
            const stdAssertion: Assertion = {
                urn: 'urn:li:assertion:vol-std',
                info: {
                    type: 'VOLUME' as any,
                    volumeAssertion: {
                        type: 'ROW_COUNT_TOTAL' as any,
                        rowCountTotal: {
                            operator: 'GREATER_THAN' as any,
                            parameters: { value: { value: '100', type: 'NUMBER' } },
                        },
                    },
                    source: { type: 'USER' as any },
                },
            } as any;

            const builderState = createAssertionMonitorBuilderState(stdAssertion, mockEntity, mockMonitor);

            // Should keep standard type
            expect(builderState.assertion?.volumeAssertion?.type).toBe('ROW_COUNT_TOTAL');

            // 2. Saving
            const variables = builderStateToSharedVolumeAssertionVariables(builderState);
            expect(variables.type).toBe('ROW_COUNT_TOTAL');
            expect(variables).toHaveProperty('rowCountTotal.operator', 'GREATER_THAN');
        });
    });

    describe('Field Metric Assertion Round-Trip', () => {
        it('should correctly handle AI Inferred Field Metric Assertion', () => {
            // 1. Loading
            const aiAssertion: Assertion = {
                urn: 'urn:li:assertion:field-ai',
                info: {
                    type: 'FIELD' as any,
                    fieldAssertion: {
                        type: 'FIELD_METRIC' as any,
                        fieldMetricAssertion: {
                            metric: 'NULL_COUNT' as any,
                            operator: 'BETWEEN' as any,
                        },
                    },
                    source: { type: 'INFERRED' as any },
                },
            } as any;

            const builderState = createAssertionMonitorBuilderState(aiAssertion, mockEntity, mockMonitor);

            // Should map to extended operator
            expect(builderState.assertion?.fieldAssertion?.fieldMetricAssertion?.operator).toBe(
                FieldMetricAssertionBuilderOperatorOptions.AiInferred,
            );

            // 2. Saving
            const variables = builderStateToSharedFieldAssertionVariables(builderState);

            // Should map back to standard operator
            expect(variables.fieldMetricAssertion?.operator).toBe('BETWEEN');
        });
    });
});
