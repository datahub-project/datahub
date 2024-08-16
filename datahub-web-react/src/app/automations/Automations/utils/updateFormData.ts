import { FormDataType } from '@app/automations/types';
import { unmergeTagsAndTerms } from './unmergeTagsAndTerms';

// Utility that updates the form data based on an existing action definition
// Maps existing automation definitions to the form data
export const updateFormData = (definition, formData) => {
    if (!definition) return {};

    const { action } = definition;

    // The base of all recipes
    const updatedFormData: FormDataType = {
        ...formData,
        name: definition.name || '',
    };

    // If the recipe has an action
    if (action) {
        const { config } = action;

        // If the recipe has a config in the action
        if (config) {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            const { term_propagation, tag_propagation, snowflake } = config;

            const tagsAndTerms = unmergeTagsAndTerms(
                JSON.parse(config?.term_propagation?.target_terms) ?? [],
                config?.term_propagation?.term_groups ?? [],
                config?.tag_propagation?.tag_prefixes ?? [],
            ) || {
                terms: [],
                tags: [],
                nodes: [],
            };

            const connection = config?.snowflake || {};

            // If the recipe has term propagation
            if (term_propagation || tag_propagation) {
                updatedFormData.tagsAndTerms = tagsAndTerms;
                updatedFormData.termPropagationEnabled = config?.term_propagation?.enabled ?? true;
                updatedFormData.tagPropagationEnabled = config?.tag_propagation?.enabled ?? true;
            }

            // If the recipe has a snowflake connection
            if (snowflake) updatedFormData.connection = connection;
        }
    }

    return updatedFormData;
};
