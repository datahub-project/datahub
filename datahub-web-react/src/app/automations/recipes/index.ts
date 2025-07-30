import _ from 'lodash';

import BigQueryTagSync from '@app/automations/recipes/bigQuery/metadataSync';
import DatabricksMetadataSync from '@app/automations/recipes/databricks/metadataSync';
import DocumentationColumnPropagation from '@app/automations/recipes/documentation/columnLevelPropagation';
import AIGlossaryTermPropagation from '@app/automations/recipes/glossaryTerm/glossaryTermAI';
import GlossaryTermPropagation from '@app/automations/recipes/glossaryTerm/termPropagation';
import GlossaryTermPropagationV2 from '@app/automations/recipes/glossaryTerm/termPropagationV2';
import SnowflakeTagPropagation from '@app/automations/recipes/snowflake/metadataSync';
import TagPropagation from '@app/automations/recipes/tag/tagPropagation';
import TagPropagationV2 from '@app/automations/recipes/tag/tagPropagationV2';

// Map of all templates available in the application (this needs to be in sync with the recipes)
export const templates = [
    DocumentationColumnPropagation,
    GlossaryTermPropagation,
    GlossaryTermPropagationV2,
    TagPropagation,
    TagPropagationV2,
    SnowflakeTagPropagation,
    AIGlossaryTermPropagation,
    BigQueryTagSync,
    DatabricksMetadataSync,
];

// For each automation, the default action recipe configs. This allows some defaults to be set without any associated field in the form.
export const defaultRecipes = Object.fromEntries(
    templates.map((template) => [template.key, Object.seal(template.defaultRecipe)]),
);

// Map of all config maps available in the application (this needs to be in sync with the recipes/templates)
// This is used to map the config fields to the form state
export const configMaps = Object.fromEntries(templates.map((template) => [template.key, template.configMap]));

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
