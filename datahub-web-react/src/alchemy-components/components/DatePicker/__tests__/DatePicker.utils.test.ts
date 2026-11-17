import { describe, expect, it } from 'vitest';

import { buildDateTimeFormat } from '@components/components/DatePicker/DatePicker.utils';

describe('buildDateTimeFormat', () => {
    it('appends a time portion, keeping the original as a parse fallback', () => {
        expect(buildDateTimeFormat('ll')).toEqual(['ll HH:mm:ss', 'll']);
    });

    it('appends to every format when a variant declares parse fallbacks', () => {
        expect(buildDateTimeFormat(['ll', 'YYYY-MM-DD', 'YYYY/MM/DD'])).toEqual([
            'll HH:mm:ss',
            'YYYY-MM-DD HH:mm:ss',
            'YYYY/MM/DD HH:mm:ss',
            'll',
            'YYYY-MM-DD',
            'YYYY/MM/DD',
        ]);
    });

    it('leaves a format that already carries a time untouched', () => {
        expect(buildDateTimeFormat('YYYY-MM-DD HH:mm')).toEqual(['YYYY-MM-DD HH:mm']);
    });

    it('falls back to a standalone date-time format when there is nothing to extend', () => {
        expect(buildDateTimeFormat(undefined)).toEqual(['YYYY-MM-DD HH:mm:ss']);
        expect(buildDateTimeFormat('')).toEqual(['YYYY-MM-DD HH:mm:ss']);
        expect(buildDateTimeFormat(() => 'custom')).toEqual(['YYYY-MM-DD HH:mm:ss']);
    });
});
