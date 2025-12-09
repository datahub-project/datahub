import { describe, expect, test } from 'vitest';

import { decodeSecret, encodeSecret } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/utils';

describe('utils', () => {
    describe('encodeSecret', () => {
        test('should encode a secret name with the expected format', () => {
            const secretName = 'mySecret';
            // eslint-disable-next-line no-template-curly-in-string
            const expectedResult = '${mySecret}';

            const result = encodeSecret(secretName);

            expect(result).toBe(expectedResult);
        });

        test('should encode an empty string', () => {
            const secretName = '';
            // eslint-disable-next-line no-template-curly-in-string
            const expectedResult = '${}';

            const result = encodeSecret(secretName);

            expect(result).toBe(expectedResult);
        });

        test('should handle special characters in secret name', () => {
            const secretName = 'my-secret_value.123';
            // eslint-disable-next-line no-template-curly-in-string
            const expectedResult = '${my-secret_value.123}';

            const result = encodeSecret(secretName);

            expect(result).toBe(expectedResult);
        });
    });

    describe('decodeSecret', () => {
        test('should decode an encoded secret properly', () => {
            // eslint-disable-next-line no-template-curly-in-string
            const encodedSecret = '${mySecret}';
            const expectedResult = 'mySecret';

            const result = decodeSecret(encodedSecret);

            expect(result).toBe(expectedResult);
        });

        test('should return the same value if string does not have the expected format', () => {
            const encodedSecret = 'notEncodedSecret';
            const expectedResult = 'notEncodedSecret';

            const result = decodeSecret(encodedSecret);

            expect(result).toBe(expectedResult);
        });

        // eslint-disable-next-line no-template-curly-in-string
        test('should return the same value if string starts with ${ but does not end with }', () => {
            // eslint-disable-next-line no-template-curly-in-string
            const encodedSecret = '${incomplete';
            // eslint-disable-next-line no-template-curly-in-string
            const expectedResult = '${incomplete';

            const result = decodeSecret(encodedSecret);

            expect(result).toBe(expectedResult);
        });

        test('should return the same value if string ends with } but does not start with ${', () => {
            const encodedSecret = 'incomplete}';
            const expectedResult = 'incomplete}';

            const result = decodeSecret(encodedSecret);

            expect(result).toBe(expectedResult);
        });

        test('should handle empty string', () => {
            const encodedSecret = '';
            const expectedResult = '';

            const result = decodeSecret(encodedSecret);

            expect(result).toBe(expectedResult);
        });

        test('should handle null input', () => {
            // @ts-expect-error - testing invalid input
            const result = decodeSecret(null);

            expect(result).toBeNull();
        });

        test('should handle undefined input', () => {
            // @ts-expect-error - testing invalid input
            const result = decodeSecret(undefined);

            expect(result).toBeUndefined();
        });

        test('should decode secret with special characters', () => {
            // eslint-disable-next-line no-template-curly-in-string
            const encodedSecret = '${my-secret_value.123}';
            const expectedResult = 'my-secret_value.123';

            const result = decodeSecret(encodedSecret);

            expect(result).toBe(expectedResult);
        });

        test('should decode secret with numbers only', () => {
            const encodedSecret = '${12345}'; // eslint-disable-line no-template-curly-in-string
            const expectedResult = '12345';

            const result = decodeSecret(encodedSecret);

            expect(result).toBe(expectedResult);
        });

        test('should decode secret with spaces (though probably not recommended)', () => {
            // eslint-disable-next-line no-template-curly-in-string
            const encodedSecret = '${secret with spaces}';
            const expectedResult = 'secret with spaces';

            const result = decodeSecret(encodedSecret);

            expect(result).toBe(expectedResult);
        });
    });
});
