/* 
	This file is a recipe for tag propagation to BigQuery. 
	It is used to create a new tag propagation automation in the DataHub UI.

	Action: TBD
*/
import { commonFieldsMapping } from '@app/automations/constants';
import { getField } from '@app/automations/fields';
import { AutomationRecipe, AutomationTemplate, ConfigMap } from '@app/automations/types';
import { EntityType } from '@src/types.generated';

import BigQueryLogo from '@images/bigquerylogo.png';

// Common unique ID for the action
// Used to identify the action in the backend & provide common key between template <> recipe
export const automationType = 'datahub_integrations.propagation.bigquery.tag_propagator.BigqueryTagPropagatorAction';

const automationName = 'BigQuery Metadata Sync';
const automationDescription = 'Sync tag, term, or description changes to BigQuery';

// Important: This is the form state which is taken by default, when creating a new automation of this type.
const defaultRecipe: AutomationRecipe = {
    name: automationName,
    description: automationDescription,
    category: 'Data Discovery',
    action: {
        type: automationType,
        config: {
            term_propagation: {
                enabled: false,
                target_terms: [],
                term_groups: [],
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
            bigquery: {
                project_id: undefined,
                credential: {
                    project_id: undefined,
                    private_key_id: undefined,
                    private_key: undefined,
                    client_email: undefined,
                    client_id: undefined,
                },
            },
        },
    },
};

// Mapping between the UI state values and the recipe config structure
// This is used to enable dynamic updates to the recipe based on custom UI state structures
export const configMap: ConfigMap = {
    ...commonFieldsMapping,
    termsEnabled: 'action.config.term_propagation.enabled',
    tagsEnabled: 'action.config.tag_propagation.enabled',
    terms: 'action.config.term_propagation.target_terms',
    nodes: 'action.config.term_propagation.term_groups',
    tags: 'action.config.tag_propagation.tag_prefixes',
    descriptionSyncEnabled: 'action.config.description_sync.enabled',
    tableDescriptionSyncEnabled: 'action.config.description_sync.table_description_sync_enabled',
    columnDescriptionSyncEnabled: 'action.config.description_sync.column_description_sync_enabled',
    'connection.project_id': 'action.config.bigquery.project_id',
    'connection.credential.project_id': 'action.config.bigquery.credential.project_id',
    'connection.credential.private_key_id': 'action.config.bigquery.credential.private_key_id',
    'connection.credential.private_key': 'action.config.bigquery.credential.private_key',
    'connection.credential.client_email': 'action.config.bigquery.credential.client_email',
    'connection.credential.client_id': 'action.config.bigquery.credential.client_id',
    // This field controls the first radio select. We derive the recipe into this field's value when editing.
    propagationAction: {
        isVirtual: true,
        resolveVirtualFormStateFieldValue: (formState: any) => {
            // This is used to create derived formData fields using the recipe values.
            if (
                formState.descriptionSyncEnabled &&
                formState.columnDescriptionSyncEnabled &&
                !formState.tableDescriptionSyncEnabled
            ) {
                return 'COLUMN_DESCRIPTIONS';
            }
            if (
                formState.descriptionSyncEnabled &&
                formState.tableDescriptionSyncEnabled &&
                !formState.columnDescriptionSyncEnabled
            ) {
                return 'TABLE_DESCRIPTIONS';
            }
            if (formState.termsEnabled) {
                return 'GLOSSARY_TERM_AS_POLICY_TAGS';
            }
            if (formState.tagsEnabled) {
                return 'TABLE_TAGS_AS_LABELS';
            }
            // No capabilities enabled.
            return undefined;
        },
        onChangeVirtualFormStateFieldValue: (newPropagationAction: any) => {
            // Now map back to the form state we want upon changing the form state.
            switch (newPropagationAction) {
                case 'TABLE_DESCRIPTIONS': {
                    return {
                        descriptionSyncEnabled: true,
                        tableDescriptionSyncEnabled: true,
                        columnDescriptionSyncEnabled: false,
                        termsEnabled: false,
                        tagsEnabled: false,
                    };
                }
                case 'COLUMN_DESCRIPTIONS': {
                    return {
                        descriptionSyncEnabled: true,
                        columnDescriptionSyncEnabled: true,
                        tableDescriptionSyncEnabled: false,
                        termsEnabled: false,
                        tagsEnabled: false,
                    };
                }
                case 'GLOSSARY_TERM_AS_POLICY_TAGS': {
                    return {
                        descriptionSyncEnabled: false,
                        tableDescriptionSyncEnabled: false,
                        columnDescriptionSyncEnabled: false,
                        termsEnabled: true,
                        tagsEnabled: false,
                    };
                }
                case 'TABLE_TAGS_AS_LABELS': {
                    return {
                        descriptionSyncEnabled: false,
                        tableDescriptionSyncEnabled: false,
                        columnDescriptionSyncEnabled: false,
                        termsEnabled: false,
                        tagsEnabled: true,
                    };
                }
                default:
                    throw Error(`Unrecognized propagationAction field provided! ${newPropagationAction}`);
            }
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
                            key: 'TABLE_TAGS_AS_LABELS',
                            name: 'Table Tags as Labels',
                            description: 'Sync Table Tags as Labels',
                        },
                        {
                            key: 'GLOSSARY_TERM_AS_POLICY_TAGS',
                            name: 'Column Glossary Terms as Policy Tags',
                            description: 'Sync Column Glossary Terms as Policy Tags',
                        },
                        {
                            key: 'TABLE_DESCRIPTIONS',
                            name: 'Table Descriptions',
                            description: 'Sync Table Descriptions',
                        },
                        {
                            key: 'COLUMN_DESCRIPTIONS',
                            name: 'Column Descriptions',
                            description: 'Sync Column Descriptions',
                        },
                    ],
                },
            },
        ],
    }),
    getField('select_tags_and_terms', {
        title: 'Select Tags',
        description: 'Choose the tags to sync as labels',
        controlKey: 'propagationAction',
        conditionalKey: 'TABLE_TAGS_AS_LABELS',
        fields: [
            {
                props: {
                    fieldTypes: [EntityType.Tag],
                    allowedRadios: ['all', 'some'],
                },
            },
        ],
    }),
    getField('select_tags_and_terms', {
        title: 'Select Glossary Terms & Term Groups',
        description: 'Choose the glossary terms to sync as policy tags',
        controlKey: 'propagationAction',
        conditionalKey: 'GLOSSARY_TERM_AS_POLICY_TAGS',
        fields: [
            {
                props: {
                    fieldTypes: [EntityType.GlossaryTerm, EntityType.GlossaryNode],
                    allowedRadios: ['all', 'some'],
                },
            },
        ],
    }),
    getField('select_connection', {
        title: 'Configure Connection',
        description: 'Provide BigQuery connection details to use for metadata sync',
        fields: [
            {
                props: {
                    connectionTypes: ['bigquery'],
                    isInlineForm: true,
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
    platform: 'bigquery',
    logo: BigQueryLogo,
    name: automationName,
    defaultRecipe,
    description: automationDescription,
    isBeta: true,
    fields,
};
