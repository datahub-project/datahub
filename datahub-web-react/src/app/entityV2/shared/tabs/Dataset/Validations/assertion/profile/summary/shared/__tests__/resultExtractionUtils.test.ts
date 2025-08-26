import { tryGetExpectedRangeFromAssertionAgainstAbsoluteValues } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/shared/resultExtractionUtils';

import { AssertionStdOperator, AssertionStdParameterType } from '@types';

describe('tryGetExpectedRangeFromAssertionAgainstAbsoluteValues', () => {
    // Helper function to create mock assertion parameters
    const createMockParameter = (value: string) => ({
        type: AssertionStdParameterType.Number,
        value,
    });

    // Helper function to create mock totals object
    const createMockTotals = (operator: AssertionStdOperator, parameters: any) => ({
        operator,
        parameters,
    });

    describe('Between operator', () => {
        it('should return high and low values with inclusive range types', () => {
            const totals = createMockTotals(AssertionStdOperator.Between, {
                minValue: createMockParameter('10'),
                maxValue: createMockParameter('50'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                high: 50,
                low: 10,
                context: {
                    highType: 'inclusive',
                    lowType: 'inclusive',
                },
            });
        });

        it('should handle decimal values', () => {
            const totals = createMockTotals(AssertionStdOperator.Between, {
                minValue: createMockParameter('10.5'),
                maxValue: createMockParameter('50.75'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                high: 50.75,
                low: 10.5,
                context: {
                    highType: 'inclusive',
                    lowType: 'inclusive',
                },
            });
        });

        it('should handle missing minValue', () => {
            const totals = createMockTotals(AssertionStdOperator.Between, {
                maxValue: createMockParameter('50'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                high: 50,
                low: undefined,
                context: {
                    highType: 'inclusive',
                    lowType: 'inclusive',
                },
            });
        });

        it('should handle missing maxValue', () => {
            const totals = createMockTotals(AssertionStdOperator.Between, {
                minValue: createMockParameter('10'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                high: undefined,
                low: 10,
                context: {
                    highType: 'inclusive',
                    lowType: 'inclusive',
                },
            });
        });
    });

    describe('GreaterThan operator', () => {
        it('should return low value with exclusive range type', () => {
            const totals = createMockTotals(AssertionStdOperator.GreaterThan, {
                value: createMockParameter('100'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: undefined,
                notEqual: undefined,
                high: undefined,
                low: 100,
                context: {
                    highType: undefined,
                    lowType: 'exclusive',
                },
            });
        });
    });

    describe('GreaterThanOrEqualTo operator', () => {
        it('should return low value with inclusive range type', () => {
            const totals = createMockTotals(AssertionStdOperator.GreaterThanOrEqualTo, {
                value: createMockParameter('100'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: undefined,
                notEqual: undefined,
                high: undefined,
                low: 100,
                context: {
                    highType: undefined,
                    lowType: 'inclusive',
                },
            });
        });
    });

    describe('LessThan operator', () => {
        it('should return high value with exclusive range type', () => {
            const totals = createMockTotals(AssertionStdOperator.LessThan, {
                value: createMockParameter('200'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: undefined,
                notEqual: undefined,
                high: 200,
                low: undefined,
                context: {
                    highType: 'exclusive',
                    lowType: undefined,
                },
            });
        });
    });

    describe('LessThanOrEqualTo operator', () => {
        it('should return high value with inclusive range type', () => {
            const totals = createMockTotals(AssertionStdOperator.LessThanOrEqualTo, {
                value: createMockParameter('200'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: undefined,
                notEqual: undefined,
                high: 200,
                low: undefined,
                context: {
                    highType: 'inclusive',
                    lowType: undefined,
                },
            });
        });
    });

    describe('EqualTo operator', () => {
        it('should return equal value', () => {
            const totals = createMockTotals(AssertionStdOperator.EqualTo, {
                value: createMockParameter('42'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: 42,
                notEqual: undefined,
                high: undefined,
                low: undefined,
                context: {
                    highType: undefined,
                    lowType: undefined,
                },
            });
        });
    });

    describe('NotEqualTo operator', () => {
        it('should return notEqual value', () => {
            const totals = createMockTotals(AssertionStdOperator.NotEqualTo, {
                value: createMockParameter('42'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: undefined,
                notEqual: 42,
                high: undefined,
                low: undefined,
                context: {
                    highType: undefined,
                    lowType: undefined,
                },
            });
        });
    });

    describe('Edge cases', () => {
        it('should return empty object when totals is undefined', () => {
            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(undefined);

            expect(result).toEqual({});
        });

        it('should return empty object when totals is null', () => {
            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(null);

            expect(result).toEqual({});
        });

        it('should return empty object when parameters is undefined', () => {
            const totals = {
                operator: AssertionStdOperator.EqualTo,
                parameters: undefined,
            } as any;

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({});
        });

        it('should return empty object when parameters is null', () => {
            const totals = {
                operator: AssertionStdOperator.EqualTo,
                parameters: null,
            } as any;

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({});
        });

        it('should handle invalid number values', () => {
            const totals = createMockTotals(AssertionStdOperator.EqualTo, {
                value: createMockParameter('not-a-number'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: undefined,
                notEqual: undefined,
                high: undefined,
                low: undefined,
                context: {
                    highType: undefined,
                    lowType: undefined,
                },
            });
        });

        it('should handle non-Number parameter type', () => {
            const totals = createMockTotals(AssertionStdOperator.EqualTo, {
                value: {
                    type: AssertionStdParameterType.String, // Not a Number type
                    value: '42',
                },
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: undefined,
                notEqual: undefined,
                high: undefined,
                low: undefined,
                context: {
                    highType: undefined,
                    lowType: undefined,
                },
            });
        });

        it('should handle missing value parameter', () => {
            const totals = createMockTotals(AssertionStdOperator.EqualTo, {
                // No value parameter
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: undefined,
                notEqual: undefined,
                high: undefined,
                low: undefined,
                context: {
                    highType: undefined,
                    lowType: undefined,
                },
            });
        });

        it('should handle null value parameter', () => {
            const totals = createMockTotals(AssertionStdOperator.EqualTo, {
                value: null,
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: undefined,
                notEqual: undefined,
                high: undefined,
                low: undefined,
                context: {
                    highType: undefined,
                    lowType: undefined,
                },
            });
        });

        it('should handle undefined operator', () => {
            const totals = {
                operator: undefined,
                parameters: {
                    value: createMockParameter('42'),
                },
            } as any;

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: undefined,
                notEqual: undefined,
                high: undefined,
                low: undefined,
                context: {
                    highType: undefined,
                    lowType: undefined,
                },
            });
        });

        it('should handle unknown operator', () => {
            const totals = {
                operator: 'UNKNOWN_OPERATOR' as any,
                parameters: {
                    value: createMockParameter('42'),
                },
            };

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: undefined,
                notEqual: undefined,
                high: undefined,
                low: undefined,
                context: {
                    highType: undefined,
                    lowType: undefined,
                },
            });
        });
    });

    describe('Zero and negative values', () => {
        it('should handle zero values', () => {
            const totals = createMockTotals(AssertionStdOperator.EqualTo, {
                value: createMockParameter('0'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: 0,
                notEqual: undefined,
                high: undefined,
                low: undefined,
                context: {
                    highType: undefined,
                    lowType: undefined,
                },
            });
        });

        it('should handle negative values', () => {
            const totals = createMockTotals(AssertionStdOperator.Between, {
                minValue: createMockParameter('-10'),
                maxValue: createMockParameter('-5'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                high: -5,
                low: -10,
                context: {
                    highType: 'inclusive',
                    lowType: 'inclusive',
                },
            });
        });
    });

    describe('Large numbers', () => {
        it('should handle large numbers', () => {
            const totals = createMockTotals(AssertionStdOperator.EqualTo, {
                value: createMockParameter('1000000'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: 1000000,
                notEqual: undefined,
                high: undefined,
                low: undefined,
                context: {
                    highType: undefined,
                    lowType: undefined,
                },
            });
        });

        it('should handle scientific notation', () => {
            const totals = createMockTotals(AssertionStdOperator.EqualTo, {
                value: createMockParameter('1e6'),
            });

            const result = tryGetExpectedRangeFromAssertionAgainstAbsoluteValues(totals);

            expect(result).toEqual({
                equal: 1000000,
                notEqual: undefined,
                high: undefined,
                low: undefined,
                context: {
                    highType: undefined,
                    lowType: undefined,
                },
            });
        });
    });
});
