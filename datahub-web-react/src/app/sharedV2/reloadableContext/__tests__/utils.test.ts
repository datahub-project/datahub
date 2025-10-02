import { getReloadableKey, KEY_SEPARATOR } from '@app/sharedV2/reloadableContext/utils';

describe('utils', () => {
    describe('getReloadableKey', () => {
        it('should return the correct key when both keyType and entryId are provided', () => {
            const key = getReloadableKey('testType', 'testId');
            expect(key).toBe(`testType${KEY_SEPARATOR}testId`);
        });

        it('should return the correct key when only keyType is provided', () => {
            const key = getReloadableKey('testType');
            expect(key).toBe(`testType${KEY_SEPARATOR}`);
        });

        it('should handle an empty string for entryId', () => {
            const key = getReloadableKey('testType', '');
            expect(key).toBe(`testType${KEY_SEPARATOR}`);
        });
    });
});
