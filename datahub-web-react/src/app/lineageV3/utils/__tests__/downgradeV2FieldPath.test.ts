import { downgradeV2FieldPath } from '@app/lineageV3/utils/downgradeV2FieldPath';

describe('downgradeV2FieldPath', () => {
    it('strips the version prefix and type annotations from a V2 field path', () => {
        expect(downgradeV2FieldPath('[version=2.0].[type=struct].address.[type=string].street')).toBe('address.street');
    });

    it('strips the key schema prefix', () => {
        expect(downgradeV2FieldPath('[version=2.0].[key=True].[type=string].id')).toBe('id');
    });

    it('returns V1 field paths unchanged', () => {
        expect(downgradeV2FieldPath('address.street')).toBe('address.street');
    });

    it('returns empty or missing paths unchanged', () => {
        expect(downgradeV2FieldPath('')).toBe('');
        expect(downgradeV2FieldPath(undefined)).toBeUndefined();
        expect(downgradeV2FieldPath(null)).toBeNull();
    });

    // Unlike the schema utils version, this one keeps bracket suffixes inside a segment.
    it('keeps bracket suffixes that are part of a segment', () => {
        expect(downgradeV2FieldPath('[version=2.0].[type=array].addresses[0].city')).toBe('addresses[0].city');
    });
});
