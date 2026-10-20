import { getEffectivePrimaryKeys } from '@app/entityV2/dataset/profile/schema/utils/primaryKeys';

import { SchemaField } from '@types';

const field = (fieldPath: string, isPartOfKey = false) => ({ fieldPath, isPartOfKey }) as SchemaField;

describe('getEffectivePrimaryKeys', () => {
    it('uses the source key, including isPartOfKey fields, over the edited key', () => {
        const schema = { primaryKeys: ['id'], fields: [field('id'), field('day', true), field('col_a')] };
        expect(getEffectivePrimaryKeys(schema, { primaryKeys: ['col_a'] })).toEqual(new Set(['id', 'day']));
    });

    it('uses the edited key when the source declares none', () => {
        const schema = { primaryKeys: [], fields: [field('id'), field('col_a')] };
        expect(getEffectivePrimaryKeys(schema, { primaryKeys: ['col_a'] })).toEqual(new Set(['col_a']));
        expect(getEffectivePrimaryKeys(schema, null)).toEqual(new Set());
    });
});
