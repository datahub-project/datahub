import { getDatasetUrnFromMonitorUrn } from '@app/entity/shared/utils';

describe('getDatasetUrnFromMonitorUrn', () => {
    it('should extract dataset URN from a valid monitor URN', () => {
        const monitorUrn =
            'urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:snowflake,brock_testing.public.sample_userdata,PROD),bf4c794a-c294-4c2e-8a5e-0c9f28eeee39)';
        const expected = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,brock_testing.public.sample_userdata,PROD)';
        expect(getDatasetUrnFromMonitorUrn(monitorUrn)).toBe(expected);
    });

    it('should return undefined for empty input', () => {
        expect(getDatasetUrnFromMonitorUrn('')).toBeUndefined();
    });

    it('should return undefined for undefined input', () => {
        expect(getDatasetUrnFromMonitorUrn(undefined)).toBeUndefined();
    });

    it('should return undefined for null input', () => {
        expect(getDatasetUrnFromMonitorUrn(null as any)).toBeUndefined();
    });

    it('should return undefined for malformed monitor URN', () => {
        const malformedUrn = 'urn:li:monitor:invalid-format';
        expect(getDatasetUrnFromMonitorUrn(malformedUrn)).toBeUndefined();
    });

    it('should handle monitor URN with different dataset URN format', () => {
        const monitorUrn = 'urn:li:monitor:(urn:li:dataset:custom-format,some-uuid)';
        const expected = 'urn:li:dataset:custom-format';
        expect(getDatasetUrnFromMonitorUrn(monitorUrn)).toBe(expected);
    });
});
