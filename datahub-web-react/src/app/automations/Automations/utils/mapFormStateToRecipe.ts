import { configMaps, defaultRecipes, mapFormStateToActionConfig } from '../../recipes';

// Utility that updates the recipe of an automation with the new form data
export const mapFormStateToRecipe = (formState: any) => {
    const defaultRecipe = defaultRecipes[formState?.type];
    return {
        name: formState.name || defaultRecipe?.name || '',
        description: formState.description || defaultRecipe?.description || '',
        category: formState.category || defaultRecipe?.category || '',
        executorId: formState.executorId || defaultRecipe?.executorId,
        action: {
            type: formState.type,
            config: mapFormStateToActionConfig(formState, configMaps[formState?.type], defaultRecipe?.action?.config),
        },
    };
};
