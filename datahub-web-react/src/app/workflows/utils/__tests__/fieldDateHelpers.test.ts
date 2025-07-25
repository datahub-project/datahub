import moment from 'moment';

import {
    convertDatePickerToFieldValue,
    convertFieldValueToMoment,
    convertMomentToFieldValue,
} from '@app/workflows/utils/fieldDateHelpers';

describe('fieldDateHelpers', () => {
    describe('convertFieldValueToMoment', () => {
        it('should convert Date objects to moment', () => {
            const date = new Date('2023-12-25T10:30:00Z');
            const result = convertFieldValueToMoment(date);

            expect(result).toBeTruthy();
            expect(moment.isMoment(result)).toBe(true);
            expect(result!.toISOString()).toBe('2023-12-25T10:30:00.000Z');
        });

        it('should convert timestamp numbers to moment', () => {
            const timestamp = 1703505000000; // 2023-12-25T10:30:00Z
            const result = convertFieldValueToMoment(timestamp);

            expect(result).toBeTruthy();
            expect(moment.isMoment(result)).toBe(true);
            expect(result!.valueOf()).toBe(timestamp);
        });

        it('should convert string numbers to moment', () => {
            const timestampString = '1703505000000'; // 2023-12-25T10:30:00Z
            const result = convertFieldValueToMoment(timestampString);

            expect(result).toBeTruthy();
            expect(moment.isMoment(result)).toBe(true);
            expect(result!.valueOf()).toBe(1703505000000);
        });

        it('should return null for null input', () => {
            const result = convertFieldValueToMoment(null);
            expect(result).toBeNull();
        });

        it('should return null for undefined input', () => {
            const result = convertFieldValueToMoment(undefined as any);
            expect(result).toBeNull();
        });

        it('should return null for empty string', () => {
            const result = convertFieldValueToMoment('');
            expect(result).toBeNull();
        });

        it('should handle non-numeric strings (creates moment but may be invalid)', () => {
            const result = convertFieldValueToMoment('not-a-number');

            // Should create a moment object but it will be invalid
            expect(result).toBeTruthy();
            expect(moment.isMoment(result)).toBe(true);
            // moment(NaN) creates an invalid moment
            expect(result!.isValid()).toBe(false);
        });

        it('should handle boolean true (converts to 1)', () => {
            const result = convertFieldValueToMoment(true);

            expect(result).toBeTruthy();
            expect(moment.isMoment(result)).toBe(true);
            expect(result!.valueOf()).toBe(1); // moment(1) = Jan 1, 1970 00:00:00.001 GMT
        });

        it('should handle boolean false (returns null for falsy value)', () => {
            const result = convertFieldValueToMoment(false);

            expect(result).toBeNull();
        });

        it('should handle arrays (original behavior - tries to convert to number)', () => {
            const result = convertFieldValueToMoment(['test'] as any);

            // Number(['test']) = NaN, so moment(NaN) will be invalid
            expect(result).toBeTruthy();
            expect(moment.isMoment(result)).toBe(true);
            expect(result!.isValid()).toBe(false);
        });
    });

    describe('convertDatePickerToFieldValue', () => {
        it('should convert moment object to Date', () => {
            const momentDate = moment('2023-12-25T10:30:00Z');
            const result = convertDatePickerToFieldValue(momentDate);

            expect(result).toBeInstanceOf(Date);
            expect((result as Date).toISOString()).toBe('2023-12-25T10:30:00.000Z');
        });

        it('should return null for null input', () => {
            const result = convertDatePickerToFieldValue(null);
            expect(result).toBeNull();
        });

        it('should return null for undefined input', () => {
            const result = convertDatePickerToFieldValue(undefined);
            expect(result).toBeNull();
        });

        it('should handle invalid moment objects', () => {
            const invalidMoment = moment('invalid-date');
            const result = convertDatePickerToFieldValue(invalidMoment);

            // Invalid moment.toDate() still returns a Date object, but it's Invalid Date
            expect(result).toBeInstanceOf(Date);
            expect(Number.isNaN((result as Date).getTime())).toBe(true);
        });
    });

    describe('convertMomentToFieldValue (deprecated)', () => {
        it('should convert moment object to Date', () => {
            const momentDate = moment('2023-12-25T10:30:00Z');
            const result = convertMomentToFieldValue(momentDate);

            expect(result).toBeInstanceOf(Date);
            expect((result as Date).toISOString()).toBe('2023-12-25T10:30:00.000Z');
        });

        it('should return null for null input', () => {
            const result = convertMomentToFieldValue(null);
            expect(result).toBeNull();
        });

        it('should handle invalid moment objects', () => {
            const invalidMoment = moment('invalid-date');
            const result = convertMomentToFieldValue(invalidMoment);

            // Invalid moment.toDate() still returns a Date object, but it's Invalid Date
            expect(result).toBeInstanceOf(Date);
            expect(Number.isNaN((result as Date).getTime())).toBe(true);
        });
    });

    describe('integration tests - round trip conversions', () => {
        it('should maintain date integrity in Date -> moment -> Date conversion', () => {
            const originalDate = new Date('2023-12-25T10:30:00Z');

            // Convert to moment
            const momentValue = convertFieldValueToMoment(originalDate);
            expect(momentValue).toBeTruthy();

            // Convert back to Date
            const finalDate = convertDatePickerToFieldValue(momentValue);
            expect(finalDate).toBeInstanceOf(Date);
            expect((finalDate as Date).toISOString()).toBe(originalDate.toISOString());
        });

        it('should maintain timestamp integrity in number -> moment -> Date conversion', () => {
            const originalTimestamp = 1703505000000; // 2023-12-25T10:30:00Z

            // Convert to moment
            const momentValue = convertFieldValueToMoment(originalTimestamp);
            expect(momentValue).toBeTruthy();

            // Convert to Date
            const finalDate = convertDatePickerToFieldValue(momentValue);
            expect(finalDate).toBeInstanceOf(Date);
            expect((finalDate as Date).getTime()).toBe(originalTimestamp);
        });

        it('should handle null values throughout the chain', () => {
            const momentValue = convertFieldValueToMoment(null);
            expect(momentValue).toBeNull();

            const finalDate = convertDatePickerToFieldValue(momentValue);
            expect(finalDate).toBeNull();
        });
    });

    describe('edge cases', () => {
        it('should handle extremely large timestamps', () => {
            const largeTimestamp = 8640000000000000; // Max safe JavaScript date
            const result = convertFieldValueToMoment(largeTimestamp);

            expect(result).toBeTruthy();
            expect(moment.isMoment(result)).toBe(true);
            // moment can handle this but it might be at the edge of valid dates
        });

        it('should handle negative timestamps (before epoch)', () => {
            const negativeTimestamp = -86400000; // One day before epoch
            const result = convertFieldValueToMoment(negativeTimestamp);

            expect(result).toBeTruthy();
            expect(moment.isMoment(result)).toBe(true);
            expect(result!.valueOf()).toBe(negativeTimestamp);
        });

        it('should handle zero timestamp (returns null for falsy value)', () => {
            const result = convertFieldValueToMoment(0);

            expect(result).toBeNull();
        });
    });
});
