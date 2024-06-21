import { jsonToYaml } from '../ingest/source/utils';

// Utils for Automations Center

export enum AutomationTypes {
    TEST = 'Test',
    ACTION = 'ActionPipeline',
    INGESTION = 'IngestionPipeline',
}

export enum AutomationStatus {
    ACTIVE = 'active',
    RUNNING = 'running',
    FAILED = 'failed',
    STOPPED = 'stopped',
}

export const titleCase = (input) => {
    return input
        .split('_')
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
};

export const truncateString = (str, maxLength) => {
    if (str.length > maxLength) return `${str.substring(0, maxLength)}…`;
    return str;
};

// Util to safely parse JSON
export const parseJSON = (jsonString) => {
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
            description: item.description,
            category: item.category || 'Propagation',
            definition: item.details?.config?.recipe || item.definition?.json,
            type: item.__typename,
            updated: new Date(),
            created: new Date(),
        };
    });

// Fill YAML with form data
export const getYaml = (automation: any, formData: any) => {
    if (!automation) return '';

    const baseRecipe = automation?.baseRecipe;

    if (automation.type === AutomationTypes.ACTION) {
        baseRecipe.name = formData.name || '';
        if (baseRecipe.action && baseRecipe.action.config.term_propagation) {
            baseRecipe.action.config.term_propagation.target_terms = formData.terms || '';

            // handle setting config values for snowflake
            if (formData.connection) {
                baseRecipe.action.config.snowflake = formData.connection;
                baseRecipe.action.config.snowflake.password = '';
            }
        }
    }

    const json = JSON.stringify(baseRecipe, null, 2);

    return jsonToYaml(json);
};
