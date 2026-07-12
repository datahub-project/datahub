import { describe, expect, it } from 'vitest';

import { FieldType } from '@app/ingestV2/source/builder/RecipeForm/common';
import { SchemaFormField, adaptField } from '@app/ingestV2/source/builder/SchemaForm/adaptField';

describe('adaptField', () => {
    it('maps a secret field to a single SECRET RecipeField', () => {
        const field: SchemaFormField = {
            name: 'password',
            label: 'Password',
            field_path: 'source.config.password',
            widget: 'password',
            secret: true,
        };

        const result = adaptField(field);

        expect(result).toHaveLength(1);
        expect(result[0].type).toBe(FieldType.SECRET);
        expect(result[0].fieldPath).toBe('source.config.password');
    });

    it('maps an allow_deny field to two LIST RecipeFields', () => {
        const field: SchemaFormField = {
            name: 'schema_pattern',
            label: 'Schema Pattern',
            field_path: 'source.config.schema_pattern',
            widget: 'allow_deny',
        };

        const result = adaptField(field);

        expect(result).toHaveLength(2);
        expect(result[0].type).toBe(FieldType.LIST);
        expect(result[1].type).toBe(FieldType.LIST);
        expect(result[0].fieldPath).toBe('source.config.schema_pattern.allow');
        expect(result[1].fieldPath).toBe('source.config.schema_pattern.deny');
        expect(result[0].label).toBe('Schema Pattern — Allow');
        expect(result[1].label).toBe('Schema Pattern — Deny');
        expect(result[0].name).toBe('schema_pattern.allow');
        expect(result[1].name).toBe('schema_pattern.deny');
    });

    it('derives a rejecting rule for exclusive_minimum constraints', async () => {
        const field: SchemaFormField = {
            name: 'sample_size',
            label: 'Sample Size',
            field_path: 'source.config.profiling.sample_size',
            widget: 'number',
            constraints: { exclusive_minimum: 0 },
        };

        const [rf] = adaptField(field);

        expect(rf.rules).not.toBeNull();
        const rule = (rf.rules ?? []).find(
            (r): r is { validator: (rule: unknown, value: number) => Promise<void> } =>
                typeof (r as { validator?: unknown }).validator === 'function',
        );
        expect(rule).toBeDefined();
        await expect(rule?.validator(undefined, 0)).rejects.toThrow();
        await expect(rule?.validator(undefined, 1)).resolves.toBeUndefined();
    });

    it('maps depends_on/enabled_when to dynamicHidden', () => {
        const field: SchemaFormField = {
            name: 'view_pattern',
            label: 'View Pattern',
            field_path: 'source.config.view_pattern',
            widget: 'text',
            depends_on: 'include_views',
            enabled_when: true,
        };

        const [rf] = adaptField(field);

        expect(rf.dynamicHidden).toBeDefined();
        expect(rf.dynamicHidden?.({ include_views: false })).toBe(true);
        expect(rf.dynamicHidden?.({ include_views: true })).toBe(false);
    });

    it('maps a select field to SELECT with options passed through', () => {
        const field: SchemaFormField = {
            name: 'env',
            label: 'Environment',
            field_path: 'source.config.env',
            widget: 'select',
            options: [{ label: 'PROD', value: 'PROD' }],
        };

        const [rf] = adaptField(field);

        expect(rf.type).toBe(FieldType.SELECT);
        expect(rf.options).toEqual([{ label: 'PROD', value: 'PROD' }]);
    });
});
