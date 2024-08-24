import { FormDataType } from '@app/automations/types';
import { unmergeTagsAndTerms } from './unmergeTagsAndTerms';

// Util for safe JSON parsing
const safeParse = (str: string) => {
    if (typeof str !== 'string') return str; // Already an object or not a string, no need to parse
    try {
        return JSON.parse(str);
    } catch (e) {
        return [];
    }
};

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
            const { snowflake } = config;
            const isSnowflakeConfig = !!snowflake;

            const terms = config?.term_propagation?.target_terms || config?.target_terms || [];

            const nodes = config?.term_propagation?.term_groups || config?.term_groups || [];

            const tags = config?.tag_propagation?.tag_prefixes || config?.tag_prefixes || [];

            const tagsAndTerms = unmergeTagsAndTerms(safeParse(terms), safeParse(nodes), safeParse(tags)) || {
                terms: [],
                tags: [],
                nodes: [],
            };

            // If the recipe has tags or terms defined, update the form data
            if (tagsAndTerms.terms.length > 0 || tagsAndTerms.nodes.length > 0 || tagsAndTerms.tags.length > 0) {
                updatedFormData.tagsAndTerms = tagsAndTerms;
            }

            // Update the formData with config info from server
            if (!isSnowflakeConfig) {
                // Base Config
                const hasTermsOrTermGroups = !!config?.term_groups || !!config?.target_terms;
                updatedFormData.termPropagationEnabled = (hasTermsOrTermGroups && config?.enabled) || true;
                updatedFormData.tagPropagationEnabled = (!!config?.tag_prefixes && config?.enabled) || true;
            } else {
                // If the recipe has a snowflake connection
                updatedFormData.connection = snowflake ?? {};
                updatedFormData.termPropagationEnabled = config?.term_propagation?.enabled ?? true;
                updatedFormData.tagPropagationEnabled = config?.tag_propagation?.enabled ?? true;
            }
        }
    }

    return updatedFormData;
};
