import { describe, expect, test } from 'vitest';

import { encodeSecret } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/utils';

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
});
