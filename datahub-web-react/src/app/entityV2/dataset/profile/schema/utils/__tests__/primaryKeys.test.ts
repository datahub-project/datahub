import { getEffectivePrimaryKeys, isPrimaryKeyField } from '@app/entityV2/dataset/profile/schema/utils/primaryKeys';

import { SchemaField } from '@types';

const field = (fieldPath: string, isPartOfKey = false) => ({ fieldPath, isPartOfKey }) as SchemaField;

describe('getEffectivePrimaryKeys', () => {
    it('uses the source key, including isPartOfKey fields, over the edited key', () => {
        const schema = { primaryKeys: ['id'], fields: [field('id'), field('day', true), field('col_a')] };
        const keys = getEffectivePrimaryKeys(schema, ['col_a']);
        expect(['id', 'day', 'col_a'].map((path) => isPrimaryKeyField(keys, field(path)))).toEqual([true, true, false]);
    });

    it('matches source keys exactly', () => {
        const keyField = field('[version=2.0].[key=True].[type=Key].[type=string].id');
        const valueField = field('[version=2.0].[type=Value].[type=string].id');
        const keys = getEffectivePrimaryKeys(
            { primaryKeys: [keyField.fieldPath], fields: [keyField, valueField] },
            null,
        );
        expect(isPrimaryKeyField(keys, keyField)).toBe(true);
        expect(isPrimaryKeyField(keys, valueField)).toBe(false);
        expect(isPrimaryKeyField(getEffectivePrimaryKeys({ primaryKeys: ['id'], fields: [] }, null), field('ID'))).toBe(
            false,
        );
    });

    it('uses the edited key when the source declares none', () => {
        const schema = { primaryKeys: [], fields: [field('id'), field('col_a')] };
        expect(isPrimaryKeyField(getEffectivePrimaryKeys(schema, ['col_a']), field('col_a'))).toBe(true);
        expect(getEffectivePrimaryKeys(schema, null).size).toBe(0);
    });

    it('matches an edited key against v2 field paths', () => {
        const v2Field = field('[version=2.0].[type=Record].[type=string].id');
        expect(isPrimaryKeyField(getEffectivePrimaryKeys({ fields: [v2Field] }, ['id']), v2Field)).toBe(true);
    });

    it('keeps isPartOfKey badges when there is no schemaMetadata', () => {
        expect(isPrimaryKeyField(getEffectivePrimaryKeys(null, null), field('id', true))).toBe(true);
        expect(isPrimaryKeyField(getEffectivePrimaryKeys(null, null), field('col_a'))).toBe(false);
    });
});
