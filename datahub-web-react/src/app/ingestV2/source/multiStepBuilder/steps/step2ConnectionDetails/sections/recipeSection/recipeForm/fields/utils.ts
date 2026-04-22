import { RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

export function resolveDynamicOptions<T extends RecipeField>(field: T, values: Record<string, any>): T {
    let resolvedField = field;
    if (field.dynamicRequired) {
        resolvedField = { ...resolvedField, required: field.dynamicRequired(values) };
    }

    if (field.dynamicHidden) {
        resolvedField = { ...resolvedField, hidden: field.dynamicHidden(values) };
    }

    if (field.dynamicLabel) {
        resolvedField = { ...resolvedField, label: field.dynamicLabel(values) };
    }

    if (field.dynamicDisabled) {
        resolvedField = { ...resolvedField, disabled: field.dynamicDisabled(values) };
    }

    return resolvedField;
}
