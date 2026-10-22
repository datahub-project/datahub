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

    it('strips bracket suffixes attached to a name', () => {
        expect(downgradeV2FieldPath('[version=2.0].[type=array].addresses[0].city')).toBe('addresses.city');
        expect(downgradeV2FieldPath('data[0][1].value')).toBe('data.value');
        expect(downgradeV2FieldPath('event.custom_fields[user_id]')).toBe('event.custom_fields');
    });

    it('keeps brackets that are part of a column name', () => {
        expect(downgradeV2FieldPath('DOC [mg/L C]')).toBe('DOC [mg/L C]');
        expect(downgradeV2FieldPath('Temp [C]')).not.toBe(downgradeV2FieldPath('Temp [F]'));
    });
});
