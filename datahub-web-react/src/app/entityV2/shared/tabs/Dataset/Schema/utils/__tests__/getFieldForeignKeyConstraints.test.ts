import { describe, expect, it } from 'vitest';

import { sampleSchemaWithPkFk } from '@app/entityV2/dataset/profile/stories/sampleSchema';
import getFieldForeignKeyConstraints from '@app/entityV2/shared/tabs/Dataset/Schema/utils/getFieldForeignKeyConstraints';

describe('getFieldForeignKeyConstraints', () => {
    it('returns constraints where the field is a source field', () => {
        const result = getFieldForeignKeyConstraints(sampleSchemaWithPkFk, 'shipping_address');

        expect(result).toHaveLength(1);
        expect(result[0].name).toEqual('constraint');
    });

    it('trims whitespace before comparing field paths', () => {
        expect(getFieldForeignKeyConstraints(sampleSchemaWithPkFk, ' shipping_address ')).toHaveLength(1);
    });

    it('returns an empty array for a field without foreign keys', () => {
        expect(getFieldForeignKeyConstraints(sampleSchemaWithPkFk, 'id')).toEqual([]);
    });

    it('returns an empty array when schema metadata is missing', () => {
        expect(getFieldForeignKeyConstraints(null, 'shipping_address')).toEqual([]);
        expect(getFieldForeignKeyConstraints(undefined, 'shipping_address')).toEqual([]);
    });
});
