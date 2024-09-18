import { configMaps, defaultConfigs, mapFormToConfig } from '../../recipes';

// Utility that updates the recipe of an automation with the new form data
export const updateRecipe = (recipe, formData: any) => {
    if (!recipe.action) return {};
    const { action } = recipe;

    return {
        ...recipe,
        name: formData.name || recipe?.name,
        action: {
            ...action,
            config: mapFormToConfig(formData, configMaps[action?.type], defaultConfigs[action?.type]),
        },
    };
};
