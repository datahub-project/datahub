import { configMaps, mapDefinitionToState } from '@app/automations/recipes';

// Utility that updates the form data based on an existing action definition
export const updateFormData = (definition: any, formData: any) => {
    if (!definition || !definition.action) return {};
    const { action } = definition;

    const configMap = configMaps[action.type];
    if (!configMap) return {};

    return mapDefinitionToState(definition, formData, configMap);
};
