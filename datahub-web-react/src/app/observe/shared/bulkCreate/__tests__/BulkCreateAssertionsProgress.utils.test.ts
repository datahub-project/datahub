import { getAssertionTypeLabel } from '@app/observe/shared/bulkCreate/BulkCreateAssertionsProgress.utils';

import { AssertionType } from '@types';

describe('BulkCreateAssertionsProgress.utils', () => {
    describe('getAssertionTypeLabel', () => {
        it('should return "Freshness" for AssertionType.Freshness', () => {
            const result = getAssertionTypeLabel(AssertionType.Freshness);
            expect(result).toBe('Freshness');
        });

        it('should return "Volume" for AssertionType.Volume', () => {
            const result = getAssertionTypeLabel(AssertionType.Volume);
            expect(result).toBe('Volume');
        });

        it('should return "Column" for AssertionType.Field', () => {
            const result = getAssertionTypeLabel(AssertionType.Field);
            expect(result).toBe('Column');
        });

        it('should return "SQL" for AssertionType.Sql', () => {
            const result = getAssertionTypeLabel(AssertionType.Sql);
            expect(result).toBe('SQL');
        });

        it('should return "Schema" for AssertionType.DataSchema', () => {
            const result = getAssertionTypeLabel(AssertionType.DataSchema);
            expect(result).toBe('Schema');
        });

        it('should return "External" for AssertionType.Dataset', () => {
            const result = getAssertionTypeLabel(AssertionType.Dataset);
            expect(result).toBe('External');
        });

        it('should return "Custom" for AssertionType.Custom', () => {
            const result = getAssertionTypeLabel(AssertionType.Custom);
            expect(result).toBe('Custom');
        });

        it('should return string representation for unknown assertion type', () => {
            // Cast to simulate an unknown assertion type
            const unknownType = 'UNKNOWN_TYPE' as AssertionType;
            const result = getAssertionTypeLabel(unknownType);
            expect(result).toBe('UNKNOWN_TYPE');
        });

        it('should handle null assertion type', () => {
            // Cast to simulate null assertion type
            const nullType = null as any as AssertionType;
            const result = getAssertionTypeLabel(nullType);
            expect(result).toBe('null');
        });

        it('should handle undefined assertion type', () => {
            // Cast to simulate undefined assertion type
            const undefinedType = undefined as any as AssertionType;
            const result = getAssertionTypeLabel(undefinedType);
            expect(result).toBe('undefined');
        });
    });
});
