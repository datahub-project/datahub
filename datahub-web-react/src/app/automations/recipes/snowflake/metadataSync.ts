/* 
	This file is a recipe for tag propagation to Snowflake. 
	It is used to create a new tag propagation automation in the DataHub UI.

	Action: datahub-integrations-service/src/datahub_integrations/propagation/snowflake/tag_propagator.py
*/
import { commonFieldsMapping } from '@app/automations/constants';
import { getField } from '@app/automations/fields';
import { AutomationRecipe, AutomationTemplate, ConfigMap } from '@app/automations/types';
import { AppConfig, EntityType } from '@src/types.generated';

import SnowflakeLogo from '@images/snowflakelogo.png';

// Common unique ID for the action
// Used to identify the action in the backend & provide common key between template <> recipe
// Changed from 'datahub_integrations.propagation.snowflake.tag_propagator.SnowflakeTagPropagatorAction'
// to use entry point name for cleaner configuration
const automationType = 'snowflake_metadata_sync';

const automationName = 'Snowflake Metadata Sync';
const automationDescription = 'Sync tag, term, or description changes to Snowflake';

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
            description_sync: {
                enabled: false,
                table_description_sync_enabled: false,
                column_description_sync_enabled: false,
            },
            snowflake: {
                account_id: undefined,
                warehouse: undefined,
                username: undefined,
                password: undefined,
                role: undefined,
                database: undefined,
                schema: undefined,
                authentication_type: 'DEFAULT_AUTHENTICATOR',
                private_key: undefined,
                private_key_password: undefined,
            },
        },
    },
};

// Mapping between the UI state values and the recipe config structure
// This is used to enable dynamic updates to the recipe based on custom UI state structures
const configMap: ConfigMap = {
    ...commonFieldsMapping,
    termsEnabled: 'action.config.term_propagation.enabled',
    tagsEnabled: 'action.config.tag_propagation.enabled',
    terms: 'action.config.term_propagation.target_terms',
    tags: 'action.config.tag_propagation.tag_prefixes',
    descriptionSyncEnabled: 'action.config.description_sync.enabled',
    tableDescriptionSyncEnabled: 'action.config.description_sync.table_description_sync_enabled',
    columnDescriptionSyncEnabled: 'action.config.description_sync.column_description_sync_enabled',
    'connection.account_id': 'action.config.snowflake.account_id',
    'connection.warehouse': 'action.config.snowflake.warehouse',
    'connection.username': 'action.config.snowflake.username',
    'connection.password': 'action.config.snowflake.password',
    'connection.role': 'action.config.snowflake.role',
    'connection.database': 'action.config.snowflake.database',
    'connection.schema': 'action.config.snowflake.schema',
    'connection.authentication_type': 'action.config.snowflake.authentication_type',
    'connection.private_key': 'action.config.snowflake.private_key',
    'connection.private_key_password': 'action.config.snowflake.private_key_password',
    // This field controls the first radio select. We derive the recipe into this field's value when editing.
    propagationAction: {
        isVirtual: true,
        resolveVirtualFormStateFieldValue: (formState: any) => {
            // This is used to create derived formData fields using the recipe values.
            if (formState.descriptionSyncEnabled) {
                return 'DESCRIPTIONS';
            }
            return 'TAGS_AND_TERMS';
        },
        onChangeVirtualFormStateFieldValue: (newPropagationAction: any) => {
            // Now map back to the form state we want upon changing the form state.
            if (newPropagationAction === 'DESCRIPTIONS') {
                return {
                    descriptionSyncEnabled: true,
                    tableDescriptionSyncEnabled: true,
                    columnDescriptionSyncEnabled: true,
                    tagsEnabled: false,
                    termsEnabled: false,
                };
            }
            return {
                descriptionSyncEnabled: false,
                tagsEnabled: true,
                termsEnabled: true,
            };
        },
    },
};

// Define UI fields for the create & edit forms
// See implementation docs for field definitions in @app/automations/fields/index
// Pro tip: `getField` allows overriding default component variables
const fields = [
    getField('radio_selector', {
        title: 'Select Action',
        description: 'Choose the types of information to sync',
        controlKey: 'propagationAction', // used to tie conditional fields below to this selector
        fields: [
            {
                props: {
                    fieldName: 'propagationAction', // this needs to match the state key
                    options: [
                        {
                            key: 'TAGS_AND_TERMS',
                            name: 'Tags & Glossary Terms',
                            description: 'Sync Tags and Glossary Terms for Tables and Columns',
                        },
                        {
                            key: 'DESCRIPTIONS',
                            name: 'Descriptions',
                            description: 'Sync descriptions for Tables and Columns as comments',
                        },
                    ],
                },
            },
        ],
    }),
    getField('select_tags_and_terms', {
        title: 'Select Tags & Terms',
        description: 'Choose which tags and glossary terms to sync',
        controlKey: 'propagationAction',
        conditionalKey: 'TAGS_AND_TERMS',
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
        title: 'Configure Connection',
        description: 'Provide Snowflake connection details to use for metadata sync',
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
const template: AutomationTemplate = {
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
    configMap,
};

export default template;
