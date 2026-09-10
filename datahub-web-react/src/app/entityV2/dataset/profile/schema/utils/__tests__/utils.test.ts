import { describe, expect, it } from 'vitest';

import { downgradeV2FieldPath, pathMatchesInsensitiveToV2 } from '@app/entityV2/dataset/profile/schema/utils/utils';

describe('downgradeV2FieldPath', () => {
    it('strips V2 bracket annotations', () => {
        expect(downgradeV2FieldPath('[version=2.0].[type=struct].address')).toBe('address');
        expect(downgradeV2FieldPath('[version=2.0].[type=struct].address.[type=struct].street')).toBe('address.street');
    });

    it('returns V1 paths unchanged', () => {
        expect(downgradeV2FieldPath('order_id')).toBe('order_id');
        expect(downgradeV2FieldPath('address.street')).toBe('address.street');
    });

    it('handles null/undefined', () => {
        expect(downgradeV2FieldPath(null)).toBeNull();
        expect(downgradeV2FieldPath(undefined)).toBeUndefined();
    });
});

describe('pathMatchesInsensitiveToV2', () => {
    it('matches identical paths', () => {
        expect(pathMatchesInsensitiveToV2('order_id', 'order_id')).toBe(true);
    });

    it('matches V1 path against V2 path (same case)', () => {
        expect(pathMatchesInsensitiveToV2('address', '[version=2.0].[type=struct].address')).toBe(true);
        expect(
            pathMatchesInsensitiveToV2('address.street', '[version=2.0].[type=struct].address.[type=struct].street'),
        ).toBe(true);
    });

    it('matches camelCase V1 profiler path against lowercased V2 schema path (ING-2174)', () => {
        // BigQuery convert_column_urns_to_lowercase=true produces lowercased schema paths,
        // while the profiler emits the original camelCase.
        const profilerPath = 'payload.additionalInfo.rawCounterpartyId';
        const schemaPath =
            '[version=2.0].[type=struct].payload.[type=struct].additionalinfo.[type=string].rawcounterpartyid';
        expect(pathMatchesInsensitiveToV2(profilerPath, schemaPath)).toBe(true);
    });

    it('does not match genuinely different fields', () => {
        expect(pathMatchesInsensitiveToV2('order_id', 'address')).toBe(false);
        expect(
            pathMatchesInsensitiveToV2('address.street', '[version=2.0].[type=struct].address.[type=struct].city'),
        ).toBe(false);
    });

    it('returns false when either path is null or undefined', () => {
        expect(pathMatchesInsensitiveToV2(null, 'address')).toBe(false);
        expect(pathMatchesInsensitiveToV2('address', undefined)).toBe(false);
        expect(pathMatchesInsensitiveToV2(null, null)).toBe(false);
    });
});
