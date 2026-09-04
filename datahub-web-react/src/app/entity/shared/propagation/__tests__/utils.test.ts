import {
    formatAttributionOrigin,
    getAttributionOrigin,
    isExternal,
    isPropagated,
} from '@app/entity/shared/propagation/utils';

describe('propagation utils', () => {
    describe('isPropagated', () => {
        it('is true only when propagated=true is present', () => {
            expect(isPropagated([{ key: 'propagated', value: 'true' }])).toBe(true);
            expect(isPropagated([{ key: 'propagated', value: 'false' }])).toBe(false);
            expect(isPropagated([{ key: 'external', value: 'true' }])).toBe(false);
            expect(isPropagated(null)).toBe(false);
        });
    });

    describe('isExternal', () => {
        it('is true only when external=true is present', () => {
            expect(isExternal([{ key: 'external', value: 'true' }])).toBe(true);
            expect(isExternal([{ key: 'propagated', value: 'true' }])).toBe(false);
            expect(isExternal(undefined)).toBe(false);
        });
    });

    describe('getAttributionOrigin', () => {
        it('returns the origin value when present', () => {
            expect(
                getAttributionOrigin([
                    { key: 'external', value: 'true' },
                    { key: 'origin', value: 'lake-formation' },
                ]),
            ).toBe('lake-formation');
            expect(getAttributionOrigin([{ key: 'external', value: 'true' }])).toBeUndefined();
        });
    });

    describe('formatAttributionOrigin', () => {
        it('turns a marker into a display label', () => {
            expect(formatAttributionOrigin('lake-formation')).toBe('Lake Formation');
            expect(formatAttributionOrigin('snowflake')).toBe('Snowflake');
            expect(formatAttributionOrigin('my_source-system')).toBe('My Source System');
        });

        it('returns undefined for empty input', () => {
            expect(formatAttributionOrigin(undefined)).toBeUndefined();
            expect(formatAttributionOrigin('')).toBeUndefined();
        });
    });
});
