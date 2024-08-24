import { FormDataType } from '@app/automations/types';

// Utility that updates the base recipe of an automation
// Maps form data to the automations definition
export const updateRecipe = (recipe, formData: FormDataType) => {
    if (!recipe) return {};

    const { action } = recipe;

    // The base of all recipes
    const updatedRecipe: any = {
        ...recipe,
        name: formData.name || recipe.name,
    };

    // If the recipe has an action
    if (action) {
        const { config } = action;

        // If the recipe has a config in the action
        if (config) {
            const { snowflake } = config;
            const isSnowflakeConfig = !!snowflake;

            const terms =
                formData?.tagsAndTerms?.terms || config?.term_propagation?.target_terms || config?.target_terms || [];

            const nodes =
                formData?.tagsAndTerms?.nodes || config?.term_propagation?.term_groups || config?.term_groups || [];

            const tags =
                formData?.tagsAndTerms?.tags || config?.tag_propagation?.tag_prefixes || config?.tag_prefixes || [];

            // Update the recipe with the new config
            if (!isSnowflakeConfig) {
                // Base Config
                updatedRecipe.action.config = {
                    enabled: true,
                };

                // If the recipe has term propagation
                if (config?.target_terms) {
                    updatedRecipe.action.config.enabled = formData?.termPropagationEnabled || true;
                    updatedRecipe.action.config.target_terms = terms;
                }

                // If the recipe has term group propagation
                if (config?.term_groups) {
                    updatedRecipe.action.config.enabled = formData?.termPropagationEnabled || true;
                    updatedRecipe.action.config.term_groups = nodes;
                }

                // If the recipe has tag propagation
                if (config.tag_prefixes) {
                    updatedRecipe.action.config.enabled = formData?.tagPropagationEnabled || true;
                    updatedRecipe.action.config.tag_prefixes = tags;
                }
            } else {
                // Snowflake config had dif config structure
                updatedRecipe.action.config = {
                    term_propagation: {
                        enabled: formData?.termPropagationEnabled || true,
                        target_terms: terms,
                        term_groups: nodes,
                    },
                    tag_propagation: {
                        enabled: formData?.tagPropagationEnabled || true,
                        tag_prefixes: tags,
                    },
                    snowflake: formData?.connection || config?.snowflake || {},
                };
            }
        }
    }

    return updatedRecipe;
};
