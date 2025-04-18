/* 
	This file is a recipe for tag propagation to Snowflake. 
	It is used to create a new tag propagation automation in the DataHub UI.

	Action: datahub-integrations-service/src/datahub_integrations/propagation/snowflake/tag_propagator.py
*/
import { commonFieldsMapping } from '@app/automations/constants';
import { getField } from '@app/automations/fields';
import { AutomationRecipe, AutomationTemplate } from '@app/automations/types';
import { AppConfig, EntityType } from '@src/types.generated';

import SnowflakeLogo from '@images/snowflakelogo.png';

// Common unique ID for the action
// Used to identify the action in the backend & provide common key between template <> recipe
export const automationType = 'datahub_integrations.propagation.snowflake.tag_propagator.SnowflakeTagPropagatorAction';

const automationName = 'Snowflake Tag Propagation';
const automationDescription = 'Sync Tags and Glossary Terms to Snowflake Table and Column Tags';

const defaultRecipe: AutomationRecipe = {
    name: automationName,
    description: automationDescription,
    category: 'Data Discovery',
    filter: {
        event_type: 'EntityChangeEvent_v1',
    },
    action: {
        type: automationType,
        config: {
            term_propagation: {
                enabled: true,
                target_terms: [],
            },
            tag_propagation: {
                enabled: true,
                tag_prefixes: [],
            },
            snowflake: {
                account_id: undefined,
                warehouse: undefined,
                username: undefined,
                password: undefined,
                role: undefined,
                database: undefined,
                schema: undefined,
            },
        },
    },
};

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

// Define UI fields for the create & edit forms
// See implementation docs for field definitions in @app/automations/fields/index
// Pro tip: `getField` allows overriding default component variables
const fields = [
    getField('select_tags_and_terms', {
        title: 'Tags & Glossary Terms',
        description: 'Choose the tags and glossary terms to propagate to Snowflake.',
        fields: [
            {
                props: {
                    fieldTypes: [EntityType.Tag, EntityType.GlossaryTerm],
                    canShowNotice: true,
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
    getField('select_connection', {
        fields: [
            {
                props: {
                    connectionTypes: ['snowflake'],
                },
            },
        ],
    }),
    getField('details', {
        fields: [],
    }),
];

// Template for rendering all the things needed in the UI for creating/editing
// an automation based off a templated recipe system
export const template: AutomationTemplate = {
    key: automationType,
    type: automationType,
    platform: 'snowflake',
    logo: SnowflakeLogo,
    name: automationName,
    description: automationDescription,
    defaultRecipe,
    isDisabled: (appConfig: AppConfig) => !appConfig.classificationConfig.automations.snowflake,
    isBeta: true,
    fields,
};
