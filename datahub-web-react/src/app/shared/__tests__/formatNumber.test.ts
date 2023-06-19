import { formatBytes } from '../formatNumber';

describe('formatBytes', () => {
    it('should property format bytes counts', () => {
        expect(formatBytes(0)).toStrictEqual({ number: 0, unit: 'Bytes' });
        // Whole Numbers
        expect(formatBytes(1)).toStrictEqual({ number: 1, unit: 'Bytes' });
        expect(formatBytes(10)).toStrictEqual({ number: 10, unit: 'Bytes' });
        expect(formatBytes(100)).toStrictEqual({ number: 100, unit: 'Bytes' });
        expect(formatBytes(1000)).toStrictEqual({ number: 1, unit: 'KB' });
        expect(formatBytes(10000)).toStrictEqual({ number: 10, unit: 'KB' });
        expect(formatBytes(100000)).toStrictEqual({ number: 100, unit: 'KB' });
        expect(formatBytes(1000000)).toStrictEqual({ number: 1, unit: 'MB' });
        expect(formatBytes(10000000)).toStrictEqual({ number: 10, unit: 'MB' });
        expect(formatBytes(100000000)).toStrictEqual({ number: 100, unit: 'MB' });
        expect(formatBytes(1000000000)).toStrictEqual({ number: 1, unit: 'GB' });
        expect(formatBytes(10000000000)).toStrictEqual({ number: 10, unit: 'GB' });
        expect(formatBytes(100000000000)).toStrictEqual({ number: 100, unit: 'GB' });
        expect(formatBytes(1000000000000)).toStrictEqual({ number: 1, unit: 'TB' });
        expect(formatBytes(10000000000000)).toStrictEqual({ number: 10, unit: 'TB' });
        expect(formatBytes(100000000000000)).toStrictEqual({ number: 100, unit: 'TB' });
        expect(formatBytes(1000000000000000)).toStrictEqual({ number: 1, unit: 'PB' });
        // Decimal Numbers
        expect(formatBytes(12)).toStrictEqual({ number: 12, unit: 'Bytes' });
        expect(formatBytes(1200)).toStrictEqual({ number: 1.2, unit: 'KB' });
        expect(formatBytes(1200000)).toStrictEqual({ number: 1.2, unit: 'MB' });
        expect(formatBytes(1200000000)).toStrictEqual({ number: 1.2, unit: 'GB' });
        expect(formatBytes(1200000000000)).toStrictEqual({ number: 1.2, unit: 'TB' });
        expect(formatBytes(1230000000000)).toStrictEqual({ number: 1.23, unit: 'TB' });
        expect(formatBytes(1200000000000000)).toStrictEqual({ number: 1.2, unit: 'PB' });
        expect(formatBytes(1230000000000000)).toStrictEqual({ number: 1.23, unit: 'PB' });
    });
});
