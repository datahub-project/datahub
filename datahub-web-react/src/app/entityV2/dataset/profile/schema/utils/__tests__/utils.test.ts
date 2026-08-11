import { describe, expect, it } from 'vitest';

import {
    downgradeV2FieldPath,
    getParentPath,
    groupByFieldPath,
    pathMatchesInsensitiveToV2,
} from '@app/entityV2/dataset/profile/schema/utils/utils';

import { SchemaField, SchemaFieldDataType } from '@types';

function field(fieldPath: string): SchemaField {
    return {
        fieldPath,
        nullable: true,
        recursive: false,
        type: SchemaFieldDataType.String,
    };
}

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

describe('getParentPath', () => {
    it('returns null for simple top-level fields', () => {
        expect(getParentPath('order_id')).toBeNull();
    });

    it('returns struct parent for nested fields', () => {
        expect(getParentPath('[version=2.0].[type=struct].address.[type=struct].street')).toBe(
            '[version=2.0].[type=struct].address',
        );
    });

    it('skips bracket-only tokens when finding struct/array parents', () => {
        expect(getParentPath('[version=2.0].[type=struct].items.[type=array].[type=struct].sku')).toBe(
            '[version=2.0].[type=struct].items',
        );
    });

    it('drops the union variant label for qualifying union fields', () => {
        // UNION_TOKEN must be exactly 3 tokens from the end (…[type=union].Variant.field)
        expect(getParentPath('payload.[type=union].VariantA.inner')).toBe('payload.[type=union].inner');
        expect(getParentPath('[version=2.0].[type=struct].payload.[type=union].VariantA.inner')).toBe(
            '[version=2.0].[type=struct].payload.[type=union].inner',
        );
    });
});

describe('groupByFieldPath', () => {
    it('keeps top-level fields at depth 0 with no parent', () => {
        const rows = groupByFieldPath([field('a'), field('b')], { showKeySchema: false });
        expect(rows).toHaveLength(2);
        expect(rows[0].fieldPath).toBe('a');
        expect(rows[0].depth).toBe(0);
        expect(rows[0].children).toBeUndefined();
    });

    it('keeps v2 top-level fields at depth 0 when the computed parent path is absent', () => {
        const top = '[version=2.0].[type=struct].address';
        // Dot-splitting `[version=2.0]` yields a synthetic parent path that is not a real field.
        expect(getParentPath(top)).toBe('[version=2.0]');
        const rows = groupByFieldPath([field(top)], { showKeySchema: false });
        expect(rows).toHaveLength(1);
        expect(rows[0].fieldPath).toBe(top);
        expect(rows[0].depth).toBe(0);
    });

    it('nests struct children under their parent', () => {
        const parent = '[version=2.0].[type=struct].address';
        const child = '[version=2.0].[type=struct].address.[type=struct].street';
        const rows = groupByFieldPath([field(parent), field(child)], { showKeySchema: false });
        expect(rows).toHaveLength(1);
        expect(rows[0].children).toHaveLength(1);
        expect(rows[0].children![0].fieldPath).toBe(child);
        expect(rows[0].children![0].depth).toBe(1);
        expect(rows[0].children![0].parent?.fieldPath).toBe(parent);
    });

    it('nests array element fields under the array parent', () => {
        const parent = '[version=2.0].[type=struct].items';
        const child = '[version=2.0].[type=struct].items.[type=array].[type=struct].sku';
        const rows = groupByFieldPath([field(parent), field(child)], { showKeySchema: false });
        expect(rows).toHaveLength(1);
        expect(rows[0].children![0].fieldPath).toBe(child);
        expect(rows[0].children![0].depth).toBe(1);
    });

    it('nests qualifying union fields under the union parent path', () => {
        const parent = '[version=2.0].[type=struct].payload.[type=union].inner';
        const child = '[version=2.0].[type=struct].payload.[type=union].VariantA.inner';
        const rows = groupByFieldPath([field(parent), field(child)], { showKeySchema: false });
        expect(rows).toHaveLength(1);
        expect(rows[0].children![0].fieldPath).toBe(child);
        expect(rows[0].children![0].depth).toBe(1);
    });

    it('treats a field as top-level when its computed parent is missing', () => {
        const orphan = '[version=2.0].[type=struct].address.[type=struct].street';
        const rows = groupByFieldPath([field(orphan)], { showKeySchema: false });
        expect(rows).toHaveLength(1);
        expect(rows[0].fieldPath).toBe(orphan);
        expect(rows[0].depth).toBe(0);
        expect(rows[0].children).toBeUndefined();
    });
});
