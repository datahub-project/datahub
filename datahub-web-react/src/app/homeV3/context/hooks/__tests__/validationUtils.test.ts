import { describe, expect, it } from 'vitest';

import {
    validateArrayBounds,
    validateElementType,
    validatePosition,
    validateStructuredProperty,
} from '@app/homeV3/context/hooks/utils/validationUtils';

import { SummaryElementType } from '@types';

describe('Validation Utils', () => {
    describe('validatePosition', () => {
        it('should return null for valid positive position', () => {
            const result = validatePosition(0, 'test operation');
            expect(result).toBeNull();
        });

        it('should return null for valid positive position greater than 0', () => {
            const result = validatePosition(5, 'test operation');
            expect(result).toBeNull();
        });

        it('should return error message for negative position', () => {
            const result = validatePosition(-1, 'test operation');
            expect(result).toBe('Position must be non-negative for test operation');
        });

        it('should return error message for negative position with different operation name', () => {
            const result = validatePosition(-5, 'remove element');
            expect(result).toBe('Position must be non-negative for remove element');
        });

        it('should handle zero position correctly', () => {
            const result = validatePosition(0, 'replace operation');
            expect(result).toBeNull();
        });
    });

    describe('validateArrayBounds', () => {
        it('should return null when position is within bounds', () => {
            const result = validateArrayBounds(0, 5, 'test operation');
            expect(result).toBeNull();
        });

        it('should return null when position is at the last valid index', () => {
            const result = validateArrayBounds(4, 5, 'test operation');
            expect(result).toBeNull();
        });

        it('should return error when position equals array length', () => {
            const result = validateArrayBounds(5, 5, 'test operation');
            expect(result).toBe('Position is out of bounds for test operation');
        });

        it('should return error when position exceeds array length', () => {
            const result = validateArrayBounds(10, 5, 'test operation');
            expect(result).toBe('Position is out of bounds for test operation');
        });

        it('should return error when position is negative (edge case)', () => {
            const result = validateArrayBounds(-1, 5, 'test operation');
            expect(result).toBe('Position is out of bounds for test operation');
        });

        it('should handle empty array correctly', () => {
            const result = validateArrayBounds(0, 0, 'test operation');
            expect(result).toBe('Position is out of bounds for test operation');
        });

        it('should handle different operation names', () => {
            const result = validateArrayBounds(3, 2, 'remove element');
            expect(result).toBe('Position is out of bounds for remove element');
        });
    });

    describe('validateElementType', () => {
        it('should return null for valid element type', () => {
            const result = validateElementType(SummaryElementType.Created);
            expect(result).toBeNull();
        });

        it('should return null for all valid element types', () => {
            const validTypes = [
                SummaryElementType.Created,
                SummaryElementType.Owners,
                SummaryElementType.Domain,
                SummaryElementType.Tags,
                SummaryElementType.GlossaryTerms,
                SummaryElementType.StructuredProperty,
            ];

            validTypes.forEach((type) => {
                const result = validateElementType(type);
                expect(result).toBeNull();
            });
        });

        it('should return error for null element type', () => {
            const result = validateElementType(null as any);
            expect(result).toBe('Element type is required');
        });

        it('should return error for undefined element type', () => {
            const result = validateElementType(undefined as any);
            expect(result).toBe('Element type is required');
        });

        it('should return error for empty string element type', () => {
            const result = validateElementType('' as any);
            expect(result).toBe('Element type is required');
        });

        it('should return error for falsy element type', () => {
            const result = validateElementType(0 as any);
            expect(result).toBe('Element type is required');
        });
    });

    describe('validateStructuredProperty', () => {
        it('should return null for non-structured property element types', () => {
            const nonStructuredTypes = [
                SummaryElementType.Created,
                SummaryElementType.Owners,
                SummaryElementType.Domain,
                SummaryElementType.Tags,
                SummaryElementType.GlossaryTerms,
            ];

            nonStructuredTypes.forEach((type) => {
                const result = validateStructuredProperty(type, undefined);
                expect(result).toBeNull();
            });
        });

        it('should return null for non-structured property element types even without URN', () => {
            const result = validateStructuredProperty(SummaryElementType.Created);
            expect(result).toBeNull();
        });

        it('should return null for structured property with valid URN', () => {
            const result = validateStructuredProperty(
                SummaryElementType.StructuredProperty,
                'urn:li:structuredProperty:test',
            );
            expect(result).toBeNull();
        });

        it('should return error for structured property without URN', () => {
            const result = validateStructuredProperty(SummaryElementType.StructuredProperty, undefined);
            expect(result).toBe('Structured property URN is required for STRUCTURED_PROPERTY element type');
        });

        it('should return error for structured property with null URN', () => {
            const result = validateStructuredProperty(SummaryElementType.StructuredProperty, null as any);
            expect(result).toBe('Structured property URN is required for STRUCTURED_PROPERTY element type');
        });

        it('should return error for structured property with empty string URN', () => {
            const result = validateStructuredProperty(SummaryElementType.StructuredProperty, '');
            expect(result).toBe('Structured property URN is required for STRUCTURED_PROPERTY element type');
        });

        it('should return null for structured property with whitespace-only URN (truthy)', () => {
            const result = validateStructuredProperty(SummaryElementType.StructuredProperty, '   ');
            expect(result).toBeNull(); // Whitespace is truthy, so it passes
        });

        it('should handle edge case with falsy but non-empty URN', () => {
            const result = validateStructuredProperty(SummaryElementType.StructuredProperty, 0 as any);
            expect(result).toBe('Structured property URN is required for STRUCTURED_PROPERTY element type');
        });
    });
});
