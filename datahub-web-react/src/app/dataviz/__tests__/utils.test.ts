import { describe, expect, it } from 'vitest';

import { abbreviateNumber } from '@app/dataviz/utils';

describe('abbreviateNumber', () => {
    it('abbreviates into K/M/B/T bands', () => {
        expect(abbreviateNumber(1000)).toBe('1K');
        expect(abbreviateNumber(1_500_000)).toBe('1.5M');
        expect(abbreviateNumber(2_500_000_000)).toBe('2.5B');
        expect(abbreviateNumber(1_000_000_000_000)).toBe('1T');
    });

    it('strips floating-point division artifacts on fractional ticks', () => {
        // 2713.2 / 1000 = 2.7131999999999996 in IEEE-754; must render as 2.7132K.
        expect(abbreviateNumber(2713.2)).toBe('2.7132K');
    });

    it('leaves clean values unchanged (no other visible change)', () => {
        expect(abbreviateNumber(1_250_000)).toBe('1.25M');
        expect(abbreviateNumber(3_300_000)).toBe('3.3M');
        expect(abbreviateNumber(1_000_000)).toBe('1M');
    });

    it('preserves precision up to 12 significant figures', () => {
        expect(abbreviateNumber(123_456_789_012)).toBe('123.456789012B');
    });

    it('returns sub-1000 values as a number, without a suffix', () => {
        expect(abbreviateNumber(999)).toBe(999);
        expect(abbreviateNumber(42)).toBe(42);
    });

    it('also strips float artifacts below 1000 (symmetric cleanup)', () => {
        // 0.1 + 0.2 = 0.30000000000000004 in IEEE-754.
        expect(abbreviateNumber(0.1 + 0.2)).toBe(0.3);
    });

    it('returns the original string for non-numeric input', () => {
        expect(abbreviateNumber('abc')).toBe('abc');
    });
});
