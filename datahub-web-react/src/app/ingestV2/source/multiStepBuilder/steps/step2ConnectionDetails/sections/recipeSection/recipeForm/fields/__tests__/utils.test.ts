import { RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';
import {
    resolveDynamicOptions,
    writeShownFieldValues,
} from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/fields/utils';

describe('resolveDynamicOptions', () => {
    const mockField: RecipeField = {
        name: 'test-field',
        label: 'Test Field',
        tooltip: 'Test tooltip',
        type: 0, // FieldType.TEXT (TEXT enum value is 0)
        fieldPath: 'test.path',
        rules: null,
    };

    it('should return field unchanged if no dynamic properties', () => {
        const field = { ...mockField };
        const values = { someValue: 'test' };
        const result = resolveDynamicOptions(field, values);

        expect(result).toEqual(field);
    });

    it('should resolve dynamicRequired property correctly', () => {
        const field = {
            ...mockField,
            dynamicRequired: (values: Record<string, any>) => !!values?.isRequired,
        };
        const values = { isRequired: true };
        const result = resolveDynamicOptions(field, values);

        expect(result.required).toBe(true);
        expect(result.label).toBe('Test Field');
    });

    it('should resolve dynamicRequired to false when condition is not met', () => {
        const field = {
            ...mockField,
            dynamicRequired: (values: Record<string, any>) => !!values?.isRequired,
        };
        const values = { isRequired: false };
        const result = resolveDynamicOptions(field, values);

        expect(result.required).toBe(false);
    });

    it('should resolve dynamicHidden property correctly', () => {
        const field = {
            ...mockField,
            dynamicHidden: (values: Record<string, any>) => !!values?.shouldHide,
        };
        const values = { shouldHide: true };
        const result = resolveDynamicOptions(field, values);

        expect(result.hidden).toBe(true);
    });

    it('should resolve dynamicHidden to false when condition is not met', () => {
        const field = {
            ...mockField,
            dynamicHidden: (values: Record<string, any>) => !!values?.shouldHide,
        };
        const values = { shouldHide: false };
        const result = resolveDynamicOptions(field, values);

        expect(result.hidden).toBe(false);
    });

    it('should resolve dynamicLabel property correctly', () => {
        const field = {
            ...mockField,
            dynamicLabel: (values: Record<string, any>) => `Dynamic label: ${values?.suffix}`,
        };
        const values = { suffix: 'test' };
        const result = resolveDynamicOptions(field, values);

        expect(result.label).toBe('Dynamic label: test');
    });

    it('should resolve dynamicDisabled property correctly', () => {
        const field = {
            ...mockField,
            dynamicDisabled: (values: Record<string, any>) => !!values?.isDisabled,
        };
        const values = { isDisabled: true };
        const result = resolveDynamicOptions(field, values);

        expect(result.disabled).toBe(true);
    });

    it('should resolve dynamicDisabled to false when condition is not met', () => {
        const field = {
            ...mockField,
            dynamicDisabled: (values: Record<string, any>) => !!values?.isDisabled,
        };
        const values = { isDisabled: false };
        const result = resolveDynamicOptions(field, values);

        expect(result.disabled).toBe(false);
    });

    it('should handle multiple dynamic properties simultaneously', () => {
        const field = {
            ...mockField,
            required: false,
            dynamicRequired: (values: Record<string, any>) => !!values?.isRequired,
            dynamicHidden: (values: Record<string, any>) => !!values?.shouldHide,
            dynamicLabel: (values: Record<string, any>) => `Updated: ${values?.label}`,
            dynamicDisabled: (values: Record<string, any>) => !!values?.isDisabled,
        };
        const values = {
            isRequired: true,
            shouldHide: true,
            label: 'newLabel',
            isDisabled: true,
        };
        const result = resolveDynamicOptions(field, values);

        expect(result.required).toBe(true);
        expect(result.hidden).toBe(true);
        expect(result.label).toBe('Updated: newLabel');
        expect(result.disabled).toBe(true);
    });

    it('should not override static properties with dynamic ones when dynamic function is not called', () => {
        const field = {
            ...mockField,
            required: true, // static required
            dynamicRequired: () => false, // would make it false
        };
        const values = {};
        const result = resolveDynamicOptions(field, values);

        expect(result.required).toBe(false); // dynamic function should override static property
    });

    it('should handle undefined values gracefully', () => {
        const field = {
            ...mockField,
            dynamicRequired: (values: Record<string, any>) => !!values?.isRequired,
        };
        const values = undefined as any;
        const result = resolveDynamicOptions(field, values);

        expect(result.required).toBe(false); // undefined values should result in false
    });

    it('should handle null values gracefully', () => {
        const field = {
            ...mockField,
            dynamicHidden: (values: Record<string, any>) => !!values?.shouldHide,
        };
        const values = null as any;
        const result = resolveDynamicOptions(field, values);

        expect(result.hidden).toBe(false); // null values should result in false
    });
});

describe('writeShownFieldValues', () => {
    const isOnPrem = (values: Record<string, any>) => values?.deployment === 'on_prem';
    const hostField: RecipeField = {
        name: 'tenant_hostname',
        label: 'Tenant',
        tooltip: '',
        type: 0,
        fieldPath: 'source.config.tenant_hostname',
        rules: null,
        dynamicHidden: isOnPrem,
        hidden: true,
    };

    it('writes the value of a field the change shows again', () => {
        const recipe = { source: { config: {} } };
        const result = writeShownFieldValues(
            recipe,
            [hostField],
            { deployment: 'cloud' },
            { deployment: 'cloud', tenant_hostname: 'tenant.example.com' },
        );

        expect(result.source.config.tenant_hostname).toBe('tenant.example.com');
    });

    it('leaves fields that stay hidden, were visible, or are empty', () => {
        const recipe = { source: { config: {} } };
        const stillHidden = writeShownFieldValues(
            recipe,
            [hostField],
            { deployment: 'on_prem' },
            { deployment: 'on_prem', tenant_hostname: 'tenant.example.com' },
        );
        const wasVisible = writeShownFieldValues(
            recipe,
            [{ ...hostField, hidden: false }],
            { deployment: 'cloud' },
            { deployment: 'cloud', tenant_hostname: 'tenant.example.com' },
        );
        const empty = writeShownFieldValues(
            recipe,
            [hostField],
            { deployment: 'cloud' },
            { deployment: 'cloud', tenant_hostname: '' },
        );

        expect(stillHidden.source.config.tenant_hostname).toBeUndefined();
        expect(wasVisible.source.config.tenant_hostname).toBeUndefined();
        expect(empty.source.config.tenant_hostname).toBeUndefined();
    });
});
