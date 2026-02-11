import { mergeCustomHeaders } from '@app/settingsV2/personal/myIntegrations/utils/headerMergeUtils';

describe('mergeCustomHeaders', () => {
    it('should return new headers when there are no existing headers', () => {
        const newHeaders = [
            { key: 'x-api-key', value: 'abc123' },
            { key: 'x-user-id', value: '42' },
        ];

        const result = mergeCustomHeaders([], newHeaders);

        expect(result).toEqual(newHeaders);
    });

    it('should return existing headers when there are no new headers', () => {
        const existing = [
            { key: 'x-api-key', value: 'abc123' },
            { key: 'x-user-id', value: '42' },
        ];

        const result = mergeCustomHeaders(existing, []);

        expect(result).toEqual([
            { key: 'x-api-key', value: 'abc123' },
            { key: 'x-user-id', value: '42' },
        ]);
    });

    it('should overwrite existing headers with matching keys', () => {
        const existing = [
            { key: 'x-api-key', value: 'old-key' },
            { key: 'x-other', value: 'keep-me' },
        ];
        const newHeaders = [{ key: 'x-api-key', value: 'new-key' }];

        const result = mergeCustomHeaders(existing, newHeaders);

        expect(result).toEqual([
            { key: 'x-other', value: 'keep-me' },
            { key: 'x-api-key', value: 'new-key' },
        ]);
    });

    it('should handle existing headers with null keys', () => {
        const existing = [
            { key: null, value: 'null-key-header' },
            { key: 'x-valid', value: 'valid' },
        ];
        const newHeaders = [{ key: 'x-new', value: 'new-value' }];

        const result = mergeCustomHeaders(existing, newHeaders);

        expect(result).toEqual([
            { key: '', value: 'null-key-header' },
            { key: 'x-valid', value: 'valid' },
            { key: 'x-new', value: 'new-value' },
        ]);
    });

    it('should handle existing headers with undefined keys', () => {
        const existing = [{ key: undefined, value: 'undef-key-header' }];
        const newHeaders = [{ key: 'x-new', value: 'new-value' }];

        const result = mergeCustomHeaders(existing, newHeaders);

        expect(result).toEqual([
            { key: '', value: 'undef-key-header' },
            { key: 'x-new', value: 'new-value' },
        ]);
    });

    it('should return empty array when both inputs are empty', () => {
        const result = mergeCustomHeaders([], []);

        expect(result).toEqual([]);
    });

    it('should handle complete replacement of all existing headers', () => {
        const existing = [
            { key: 'x-a', value: '1' },
            { key: 'x-b', value: '2' },
        ];
        const newHeaders = [
            { key: 'x-a', value: 'new-1' },
            { key: 'x-b', value: 'new-2' },
        ];

        const result = mergeCustomHeaders(existing, newHeaders);

        expect(result).toEqual([
            { key: 'x-a', value: 'new-1' },
            { key: 'x-b', value: 'new-2' },
        ]);
    });
});
