import { configMaps, mapRecipeToFormState as mapRecipeToFormStateWithConfig } from '@app/automations/recipes';

// Utility that updates the form data based on an existing action definition
export const mapRecipeToFormState = (recipe: any, formState: any) => {
    if (!recipe || !recipe.action) return formState;
    const { action } = recipe;

    const configMap = configMaps[action.type];
    if (!configMap) return formState;

    return mapRecipeToFormStateWithConfig(
        recipe,
        {
            ...formState,
            type: action.type, // This is not always present in the formState (creates), but can be derived from the recipe.
        },
        configMap,
    );
};
