import _ from 'lodash';

import { jsonToYaml } from '@app/ingest/source/utils';

import { AUTOMATION_CATEGORY_NAME_TO_INFO } from '@app/automations/constants';
import type { AutomationTemplate } from '@app/automations/types';
import { templates } from '@app/automations/recipes';

// Function to flatten nested data using Lodash
// This will only flatten one level of nesting
// Notice: If multiple components have the same state key names, they will conflict and
// possibly overwrite.
// Assumes that each part of the state is managed by different components. This may be true, but is not necessarily.
export const extractFormState = (obj: Record<string, any>): Record<string, any> => {
    return _.transform(
        obj,
        (result, value, key) => {
            if (_.isPlainObject(value)) {
                _.assign(result, value);
            } else {
                const newResult = { ...result };
                newResult[key] = value;
                Object.assign(result, newResult);
            }
        },
        {} as Record<string, any>,
    );
};

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
export const parseJSON = (value: string): any => {
    if (typeof value === 'string') {
        try {
            return JSON.parse(value);
        } catch (error) {
            return value; // If parsing fails, return the original string
        }
    }
    return value;
};

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
export const getFields = (key: string) => templates.filter((template) => template.key === key)[0]?.fields;

// Get the data of the automation type
export const getAutomationData = (key: string) => {
    const automation = templates.filter((auto) => auto.key === key);
    return automation ? automation[0] : undefined;
};

// Get the automation template
export const getTemplate = (type: string): AutomationTemplate | undefined => {
    const template = templates.filter((t) => t.type === type);
    return template[0] || undefined;
};

// Returns true if the category name is "well-supported" (e.g. a built in), false otherwise.
export const isSupportedCategory = (categoryName: string) => {
    return AUTOMATION_CATEGORY_NAME_TO_INFO.get(categoryName) !== undefined;
};
