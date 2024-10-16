import _ from 'lodash';

import * as SnowflakeTagPropagation from './snowflake/tagPropagation';
import * as GlossaryTermPropagation from './glossaryTerm/termPropagation';
import * as AIGlossaryTermPropagation from './glossaryTerm/glossaryTermAI';
import * as DocumentationColumnPropagation from './documentation/columnLevelPropagation';
import * as BigQueryTagSync from './bigQuery/tagSync';

// Map of all templates available in the application (this needs to be in sync with the recipes)
export const templates = [
    DocumentationColumnPropagation.template,
    GlossaryTermPropagation.template,
    SnowflakeTagPropagation.template,
    AIGlossaryTermPropagation.template,
    BigQueryTagSync.template,
];

// For each automation, the default action recipe configs. This allows some defaults to be set without any associated field in the form.
export const defaultRecipes = {
    [DocumentationColumnPropagation.automationType]: Object.seal(DocumentationColumnPropagation.template.defaultRecipe),
    [GlossaryTermPropagation.automationType]: Object.seal(GlossaryTermPropagation.template.defaultRecipe),
    [SnowflakeTagPropagation.automationType]: Object.seal(SnowflakeTagPropagation.template.defaultRecipe),
    [AIGlossaryTermPropagation.automationType]: Object.seal(AIGlossaryTermPropagation.template.defaultRecipe),
    [BigQueryTagSync.automationType]: Object.seal(BigQueryTagSync.template.defaultRecipe),
};

// Map of all config maps available in the application (this needs to be in sync with the recipes/templates)
// This is used to map the config fields to the form state
export const configMaps = {
    [DocumentationColumnPropagation.automationType]: DocumentationColumnPropagation.configMap,
    [GlossaryTermPropagation.automationType]: GlossaryTermPropagation.configMap,
    [SnowflakeTagPropagation.automationType]: SnowflakeTagPropagation.configMap,
    [AIGlossaryTermPropagation.automationType]: AIGlossaryTermPropagation.configMap,
    [BigQueryTagSync.automationType]: BigQueryTagSync.configMap,
};

// Utility function to get a nested value
export const getNestedValue = (obj: any, path: string) => {
    return path.split('.').reduce((acc, part) => acc && acc[part], obj);
};

// Utility function to set a nested value
const setNestedValue = (obj: Record<string, any>, path: string, value: any) => {
    const keys = path.split('.');
    const lastKey = keys.pop()!;
    const lastObj = keys.reduce((acc, key) => {
        if (acc[key] === undefined || acc[key] === null) {
            acc[key] = {};
        }
        return acc[key];
    }, obj);

    lastObj[lastKey] = value;
    return lastObj;
};

// Function to map form data to config
export const mapFormStateToActionConfig = (
    formData: any,
    mappingConfig: Record<string, any>,
    defaultActionConfig?: Record<string, any>,
): any => {
    if (!mappingConfig) return {};

    const finalActionConfig: any = { action: { config: defaultActionConfig ? _.cloneDeep(defaultActionConfig) : {} } };

    Object.keys(mappingConfig).forEach((formKey) => {
        const configKeyOrMapping = mappingConfig[formKey];
        const formFieldValue = getNestedValue(formData, formKey);

        // Make sure we skip virtual fields :)
        if (formFieldValue !== undefined && !configKeyOrMapping?.isVirtual) {
            // Case 2: Simple mapping. A 1:1 mapping is possible between the formData and the action config.
            setNestedValue(finalActionConfig, configKeyOrMapping, formFieldValue);
        }
    });
    return finalActionConfig.action.config;
};

// Function to map configuration to form data
export const mapRecipeToFormState = (
    recipe: Record<string, any>,
    formState: Record<string, any>,
    configMap: Record<string, any>,
): Record<string, any> => {
    const result = { ...formState };

    Object.entries(configMap).forEach(([targetKey, sourceKey]) => {
        // Value is the final mapped field for the formData
        let value = _.get(recipe, sourceKey);

        // Safely parse JSON strings
        if (typeof value === 'string') {
            try {
                const parsedValue = JSON.parse(value);
                value = parsedValue;
            } catch (error) {
                // If parsing fails, retain the original string value
            }
        }

        if (value !== undefined) {
            _.set(result, targetKey, value);
        }
    });

    return result;
};
