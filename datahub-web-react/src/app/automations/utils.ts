import { jsonToYaml } from '@app/ingest/source/utils';

import { AUTOMATION_CATEGORY_NAME_TO_INFO, DEFAULT_AUTOMATION_CATEGORY } from '@app/automations/constants';
import type { AutomationTemplate } from '@app/automations/types';
import { automationTemplates } from '@app/automations/automationTemplates';

export const titleCase = (input: string) => {
    return input
        .split('_')
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
};

export const truncateString = (str: string, maxLength: number) => {
    if (str.length > maxLength) return `${str.substring(0, maxLength)}…`;
    return str;
};

// Util to safely parse JSON
export const parseJSON = (jsonString: string) => {
    try {
        const json = JSON.parse(jsonString);
        return json;
    } catch (e) {
        return {};
    }
};

export const simplifyDataForListView = (data: any) =>
    data.map((item: any) => {
        return {
            key: item.urn || titleCase(item.details?.name),
            urn: item.urn,
            name: item.name || titleCase(item.details?.name),
            description: item.details?.description,
            category: item.category || DEFAULT_AUTOMATION_CATEGORY,
            definition: item.details?.config?.recipe || item.definition?.json,
            type: item.__typename,
            updated: new Date(),
            created: new Date(),
        };
    });

// Fill YAML with form data
export const getYaml = (recipe: any) => {
    const modifiedRecipe = { ...recipe };

    // hide password
    if (recipe.action?.config?.snowflake?.password) modifiedRecipe.action.config.snowflake.password = '';

    // parse & return
    const json = JSON.stringify(modifiedRecipe, null, 2);
    return jsonToYaml(json);
};

// Get the steps for the automation type
export const getFields = (key: string) => automationTemplates.filter((automation) => automation.key === key)[0]?.fields;

// Get the data of the automation type
export const getAutomationData = (key: string) => {
    const automation = automationTemplates.filter((auto) => auto.key === key);
    return automation ? automation[0] : undefined;
};

// Get the automation template
export const getTemplate = (type: string): AutomationTemplate | undefined => {
    const template = automationTemplates.filter((auto) => auto.baseRecipe?.action?.type === type);
    return template[0] || undefined;
};

// Returns true if the category name is "well-supported" (e.g. a built in), false otherwise.
export const isSupportedCategory = (categoryName: string) => {
    return AUTOMATION_CATEGORY_NAME_TO_INFO.get(categoryName) !== undefined;
};
