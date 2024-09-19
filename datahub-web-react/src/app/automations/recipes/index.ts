import _ from 'lodash';

import * as SnowflakeTagPropagation from './snowflake/tagPropagation';
import * as GlossaryTermPropagation from './glossaryTerm/termPropagation';
import * as DocumentationColumnPropagation from './documentation/columnLevelPropagation';

// Map of all recipes available in the application (this needs to be in sync with the templates)
export const recipes = [
    DocumentationColumnPropagation.integrationRecipe,
    GlossaryTermPropagation.integrationRecipe,
    SnowflakeTagPropagation.integrationRecipe,
];

// Map of all templates available in the application (this needs to be in sync with the recipes)
export const templates = [
    DocumentationColumnPropagation.template,
    GlossaryTermPropagation.template,
    SnowflakeTagPropagation.template,
];

// Map of all the default configs available in the application (this needs to be in sync with the recipes)
export const defaultConfigs = {
    [DocumentationColumnPropagation.actionType]: Object.seal(DocumentationColumnPropagation.defaultConfig),
    [GlossaryTermPropagation.actionType]: Object.seal(GlossaryTermPropagation.defaultConfig),
    [SnowflakeTagPropagation.actionType]: Object.seal(SnowflakeTagPropagation.defaultConfig),
};

// Map of all config maps available in the application (this needs to be in sync with the recipes/templates)
// This is used to map the config fields to the form state
export const configMaps = {
    [DocumentationColumnPropagation.actionType]: DocumentationColumnPropagation.configMap,
    [GlossaryTermPropagation.actionType]: GlossaryTermPropagation.configMap,
    [SnowflakeTagPropagation.actionType]: SnowflakeTagPropagation.configMap,
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
        if (!acc[key]) {
            acc[key] = {};
        }
        return acc[key];
    }, obj);

    lastObj[lastKey] = value;
    return lastObj;
};

// Function to map form data to config
export const mapFormToConfig = (formData: any, map: Record<string, string>, defaultConfig: any): any => {
    if (!map) return {};
    const config: any = { action: { config: defaultConfig } };
    Object.keys(map).forEach((formKey) => {
        const configKey = map[formKey];
        const value = getNestedValue(formData, formKey);
        if (value !== undefined) setNestedValue(config, configKey, value);
    });
    return config?.action?.config || config;
};

// Function to map configuration to form data
export const mapDefinitionToState = (
    source: Record<string, any>,
    target: Record<string, any>,
    configMap: Record<string, string>,
): Record<string, any> => {
    const result = { ...target };

    Object.entries(configMap).forEach(([targetKey, sourceKey]) => {
        let value = _.get(source, sourceKey);

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
