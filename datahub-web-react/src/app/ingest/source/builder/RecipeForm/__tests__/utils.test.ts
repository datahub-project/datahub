import { validateURL } from '../../../utils';

describe('validateURL function', () => {
    it('should resolve if the URL is valid', async () => {
        const validator = validateURL('test');
        await expect(validator.validator(null, 'https://example.com')).resolves.toBeUndefined();
        await expect(validator.validator(null, 'http://example.com')).resolves.toBeUndefined();
        await expect(validator.validator(null, 'http://subdomain.example.com/path')).resolves.toBeUndefined();
    });

    it('should reject if the URL is invalid', async () => {
        const validator = validateURL('test url');
        await expect(validator.validator(null, 'http://example')).rejects.toThrowError('A valid test url is required.');
        await expect(validator.validator(null, 'example')).rejects.toThrowError('A valid test url is required.');
        await expect(validator.validator(null, 'http://example')).rejects.toThrowError('A valid test url is required.');
    });

    it('should resolve if the value is empty', async () => {
        const validator = validateURL('test');
        await expect(validator.validator(null, '')).resolves.toBeUndefined();
        await expect(validator.validator(null, undefined)).resolves.toBeUndefined();
        await expect(validator.validator(null, null)).resolves.toBeUndefined();
    });
});
