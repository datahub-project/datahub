import { RecipeField, setFieldValueOnRecipe } from '@app/ingestV2/source/builder/RecipeForm/common';

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

// A change that shows hidden fields again, such as switching a deployment or auth
// mode back, writes only the changed field; the shown fields keep their values in
// the form, so write those values to the recipe too or the two disagree.
export function writeShownFieldValues(
    recipe: any,
    fields: RecipeField[],
    changedValues: Record<string, any>,
    allValues: Record<string, any>,
): any {
    let updatedRecipe = recipe;
    fields.forEach((field) => {
        if (!field.dynamicHidden || !field.hidden || field.name in changedValues) return;
        if (field.dynamicHidden(allValues)) return;
        const value = allValues[field.name];
        if (value === undefined || value === null || value === '') return;
        updatedRecipe = field.setValueOnRecipeOverride
            ? field.setValueOnRecipeOverride(updatedRecipe, value)
            : setFieldValueOnRecipe(updatedRecipe, value, field.fieldPath);
    });
    return updatedRecipe;
}
