import { describe, expect, it } from 'vitest';

import { deriveTypeParts, formatPrecisionScale } from '@app/entityV2/columnView/deriveTypeParts';

describe('deriveTypeParts', () => {
    it('reads precision and scale from two parameters regardless of type', () => {
        expect(deriveTypeParts('DECIMAL(18,2)', 'NUMBER')).toEqual({ precision: 18, scale: 2 });
        expect(deriveTypeParts('NUMBER(18, 2)', 'NUMBER')).toEqual({ precision: 18, scale: 2 });
        expect(deriveTypeParts('numeric(12,4)', undefined)).toEqual({ precision: 12, scale: 4 });
    });

    it('reads a single parameter as a length for string and bytes types', () => {
        expect(deriveTypeParts('VARCHAR(255)', 'STRING')).toEqual({ length: 255 });
        expect(deriveTypeParts('VARBINARY(16)', 'BYTES')).toEqual({ length: 16 });
        expect(deriveTypeParts('VARCHAR(16777216)', 'STRING')).toEqual({ length: 16777216 });
    });

    it('reads a single parameter as a precision for numeric and temporal types', () => {
        expect(deriveTypeParts('NUMBER(18)', 'NUMBER')).toEqual({ precision: 18 });
        expect(deriveTypeParts('FLOAT(53)', 'NUMBER')).toEqual({ precision: 53 });
        expect(deriveTypeParts('TIMESTAMP_NTZ(9)', 'TIME')).toEqual({ precision: 9 });
    });

    it('falls back to the native family name when the normalized type is unknown', () => {
        expect(deriveTypeParts('nvarchar(50)', null)).toEqual({ length: 50 });
        expect(deriveTypeParts('decimal(10)', null)).toEqual({ precision: 10 });
    });

    it('yields nothing for bare or unparameterized types', () => {
        expect(deriveTypeParts('string', 'STRING')).toEqual({});
        expect(deriveTypeParts('ARRAY<STRING>', 'ARRAY')).toEqual({});
        expect(deriveTypeParts(undefined, 'STRING')).toEqual({});
        expect(deriveTypeParts('', 'STRING')).toEqual({});
    });

    it('formats precision / scale for display', () => {
        expect(formatPrecisionScale({ precision: 18, scale: 2 })).toBe('18,2');
        expect(formatPrecisionScale({ precision: 18 })).toBe('18');
        expect(formatPrecisionScale({ length: 255 })).toBe('');
    });
});
