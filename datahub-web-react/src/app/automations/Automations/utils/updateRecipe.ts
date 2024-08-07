// Utility that updates the base recipe of an automation
// Maps form data to the automations definition
export const updateRecipe = (recipe, formData) => {
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
            // eslint-disable-next-line @typescript-eslint/naming-convention
            const { term_propagation, tag_propagation, snowflake } = config;

            const terms = formData?.tagsAndTerms?.terms || config?.term_propagation?.target_terms || [];
            const nodes = formData?.tagsAndTerms?.nodes || config?.term_propagation?.term_groups || [];
            const tags = formData.tagsAndTerms?.tags || config?.tag_propagation?.tag_prefixes || [];

            const connection = formData.connection || config?.snowflake || {};

            // If the recipe has term or term group propagation
            if (term_propagation) {
                updatedRecipe.action.config.term_propagation.enabled = true;
                updatedRecipe.action.config.term_propagation.target_terms = terms;
                updatedRecipe.action.config.term_propagation.term_groups = nodes;
            }

            // If the recipe has tag propagation
            if (tag_propagation) {
                updatedRecipe.action.config.tag_propagation.enabled = true;
                updatedRecipe.action.config.tag_propagation.tag_prefixes = tags;
            }

            // If the recipe has a snowflake connection
            if (snowflake) updatedRecipe.action.config.snowflake = connection;
        }
    }

    return updatedRecipe;
};
