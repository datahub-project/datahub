import { isPrimaryKeyField } from '@app/entityV2/dataset/profile/schema/components/InteriorTitleContent';

import { PrimaryKeySource } from '@types';

describe('isPrimaryKeyField', () => {
    it('uses the source primary key and isPartOfKey', () => {
        expect(isPrimaryKeyField('id', { isPartOfKey: false }, { primaryKeys: ['id'] }, null)).toBe(true);
        expect(isPrimaryKeyField('id', { isPartOfKey: true }, null, null)).toBe(true);
        expect(isPrimaryKeyField('col_a', { isPartOfKey: false }, { primaryKeys: ['id'] }, null)).toBe(false);
    });

    it('uses the declared primary key only when it is the effective key', () => {
        const declared = { primaryKeySource: PrimaryKeySource.Declared, effectivePrimaryKey: ['col_a'] };
        expect(isPrimaryKeyField('col_a', { isPartOfKey: false }, { primaryKeys: [] }, declared)).toBe(true);

        const source = { primaryKeySource: PrimaryKeySource.Source, effectivePrimaryKey: ['id'] };
        expect(isPrimaryKeyField('col_a', { isPartOfKey: false }, { primaryKeys: ['id'] }, source)).toBe(false);
    });
});
