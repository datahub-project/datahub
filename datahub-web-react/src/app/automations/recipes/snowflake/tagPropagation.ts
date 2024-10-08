/* 
	This file is a recipe for tag propagation to Snowflake. 
	It is used to create a new tag propagation automation in the DataHub UI.

	Action: datahub-integrations-service/src/datahub_integrations/propagation/snowflake/tag_propagator.py
*/

import SnowflakeLogo from '@images/snowflakelogo.png';
import { EntityType } from '@src/types.generated';
import { AutomationTypes, commonFieldsMapping } from '@app/automations/constants';
import { getField } from '@app/automations/fields';

// Common unique ID for the action
// Used to identify the action in the backend & provide common key between template <> recipe
export const actionType = 'datahub_integrations.propagation.snowflake.tag_propagator.SnowflakeTagPropagatorAction';

// Configuration structure for the integration recipe
// Default values can be set here and will be used to populate the UI form
// This is only the information in action.config in the recipe
export const defaultConfig = {
    term_propagation: {
        enabled: true,
        target_terms: [],
    },
    tag_propagation: {
        enabled: true,
        tag_prefixes: [],
    },
    snowflake: {
        account_id: '',
        warehouse: '',
        username: '',
        password: '',
        role: '',
        database: '',
        schema: '',
    },
};

// Config type export (provides stricture typing)
export type ConfigFields = typeof defaultConfig;

// Mapping between the UI state values and the recipe config structure
// This is used to enable dynamic updates to the recipe based on custom UI state structures
export const configMap: Record<string, string> = {
    ...commonFieldsMapping,
    termsEnabled: 'action.config.term_propagation.enabled',
    tagsEnabled: 'action.config.tag_propagation.enabled',
    terms: 'action.config.term_propagation.target_terms',
    tags: 'action.config.tag_propagation.tag_prefixes',
    'connection.account_id': 'action.config.snowflake.account_id',
    'connection.warehouse': 'action.config.snowflake.warehouse',
    'connection.username': 'action.config.snowflake.username',
    'connection.password': 'action.config.snowflake.password',
    'connection.role': 'action.config.snowflake.role',
    'connection.database': 'action.config.snowflake.database',
    'connection.schema': 'action.config.snowflake.schema',
};

// Recipe that's sent in JSON format to the integration service to create or update an automation
// This structure has to match what's expected in the action recipe
export const integrationRecipe = {
    name: 'Snowflake Tag Propagation',
    description: 'Sync Tags and Glossary Terms to Snowflake Table and Column Tags',
    executorId: 'default',
    filter: {
        event_type: 'EntityChangeEvent_v1',
    },
    action: {
        type: actionType,
        config: defaultConfig as ConfigFields,
    },
};

// Define UI fields for the create & edit forms
// See implementation docs for field definitions in @app/automations/fields/index
// Pro tip: `getField` allows overriding default component variables
const fields = [
    getField('select_tags_and_terms', {
        title: 'Select Tags & Glossary Terms',
        description: 'Choose the tags and glossary terms to propagate to Snowflake.',
        fields: [
            {
                props: {
                    fieldTypes: [EntityType.Tag, EntityType.GlossaryTerm],
                },
                state: {
                    terms: [],
                    tags: [],
                    termsEnabled: true,
                    tagsEnabled: true,
                },
            },
        ],
    }),
    getField('select_connection'),
    getField('details', {
        fields: [
            {
                state: {
                    name: integrationRecipe.name,
                    description: integrationRecipe.description,
                    executorId: integrationRecipe.executorId,
                },
            },
        ],
    }),
];

// Template for rendering all the things needed in the UI for creating/editing
// an automation based off a templated recipe system
export const template = {
    key: actionType,
    type: AutomationTypes.ACTION,
    platform: 'snowflake',
    logo: SnowflakeLogo,
    baseRecipe: integrationRecipe,
    name: integrationRecipe.name,
    description: integrationRecipe.name,
    isDisabled: false,
    isBeta: true,
    fields,
};
