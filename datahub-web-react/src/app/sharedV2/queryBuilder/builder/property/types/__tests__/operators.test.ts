import { describe, expect, it } from 'vitest';

import {
    OPERATOR_ID_TO_DETAILS,
    OperatorId,
    isUnaryOperator,
} from '@app/sharedV2/queryBuilder/builder/property/types/operators';

describe('operators', () => {
    describe('OPERATOR_ID_TO_DETAILS', () => {
        it('should contain all defined operator IDs', () => {
            const expectedIds = [
                OperatorId.EQUAL_TO,
                OperatorId.CONTAINS_STR,
                OperatorId.CONTAINS_ANY,
                OperatorId.REGEX_MATCH,
                OperatorId.STARTS_WITH,
                OperatorId.NOT,
                OperatorId.GREATER_THAN,
                OperatorId.LESS_THAN,
                OperatorId.EXISTS,
                OperatorId.IS_TRUE,
                OperatorId.IS_FALSE,
                OperatorId.SCHEMA_FIELDS_HAVE_DESCRIPTIONS,
            ];

            expectedIds.forEach((id) => {
                expect(OPERATOR_ID_TO_DETAILS.has(id)).toBe(true);
            });
        });

        it('should have correct English display names', () => {
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.EQUAL_TO).displayName).toBe('Equals');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.CONTAINS_STR).displayName).toBe('Contains');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.CONTAINS_ANY).displayName).toBe('Contains Any');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.REGEX_MATCH).displayName).toBe('Matches Regex');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.STARTS_WITH).displayName).toBe('Starts with');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.NOT).displayName).toBe('Not');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.GREATER_THAN).displayName).toBe('Greater Than');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.LESS_THAN).displayName).toBe('Less Than');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.EXISTS).displayName).toBe('Exists');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.IS_TRUE).displayName).toBe('Is True');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.IS_FALSE).displayName).toBe('Is False');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.SCHEMA_FIELDS_HAVE_DESCRIPTIONS).displayName).toBe(
                'Have Descriptions',
            );
        });

        it('should have correct English descriptions', () => {
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.EQUAL_TO).description).toBe('Exactly equals a value');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.EXISTS).description).toBe('The property exists');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.IS_TRUE).description).toBe('The property value is True');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.IS_FALSE).description).toBe('The property value is False');
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.SCHEMA_FIELDS_HAVE_DESCRIPTIONS).description).toBe(
                'All columns / fields have a description',
            );
        });

        it('should mark unary operators with operandCount of 1', () => {
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.EXISTS).operandCount).toBe(1);
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.IS_TRUE).operandCount).toBe(1);
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.IS_FALSE).operandCount).toBe(1);
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.SCHEMA_FIELDS_HAVE_DESCRIPTIONS).operandCount).toBe(1);
        });

        it('should not set operandCount for binary operators', () => {
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.EQUAL_TO).operandCount).toBeUndefined();
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.CONTAINS_STR).operandCount).toBeUndefined();
            expect(OPERATOR_ID_TO_DETAILS.get(OperatorId.GREATER_THAN).operandCount).toBeUndefined();
        });
    });

    describe('isUnaryOperator', () => {
        it('should return true for unary operators', () => {
            expect(isUnaryOperator(OperatorId.EXISTS)).toBe(true);
            expect(isUnaryOperator(OperatorId.IS_TRUE)).toBe(true);
            expect(isUnaryOperator(OperatorId.IS_FALSE)).toBe(true);
            expect(isUnaryOperator(OperatorId.SCHEMA_FIELDS_HAVE_DESCRIPTIONS)).toBe(true);
        });

        it('should return falsy for binary operators', () => {
            expect(isUnaryOperator(OperatorId.EQUAL_TO)).toBeFalsy();
            expect(isUnaryOperator(OperatorId.CONTAINS_STR)).toBeFalsy();
            expect(isUnaryOperator(OperatorId.CONTAINS_ANY)).toBeFalsy();
            expect(isUnaryOperator(OperatorId.REGEX_MATCH)).toBeFalsy();
            expect(isUnaryOperator(OperatorId.STARTS_WITH)).toBeFalsy();
            expect(isUnaryOperator(OperatorId.NOT)).toBeFalsy();
            expect(isUnaryOperator(OperatorId.GREATER_THAN)).toBeFalsy();
            expect(isUnaryOperator(OperatorId.LESS_THAN)).toBeFalsy();
        });

        it('should return false for unknown operator IDs', () => {
            expect(isUnaryOperator('unknown_op')).toBe(false);
            expect(isUnaryOperator('')).toBe(false);
            expect(isUnaryOperator(undefined)).toBe(false);
        });
    });
});
