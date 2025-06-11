/* 
    This file is a recipe for tag propagation to BigQuery. 
    It is used to create a new tag propagation automation in the DataHub UI.

    Action: TBD
*/
import { commonFieldsMapping } from '@app/automations/constants';
import { getField } from '@app/automations/fields';
import { AutomationRecipe, AutomationTemplate, ConfigMap } from '@app/automations/types';
import { EntityType } from '@src/types.generated';

import DatabricksLogo from '@images/databrickslogo.png';

// Common unique ID for the action
// TODO: Update.
export const automationType =
    'datahub_integrations.propagation.unity_catalog.tag_propagator:UnityCatalogPropagatorAction';

const automationName = 'Databricks Metadata Sync';
const automationDescription = 'Sync tag or description changes to Databricks';

const defaultRecipe: AutomationRecipe = {
    name: automationName,
    description: automationDescription,
    category: 'Data Discovery',
    action: {
        type: automationType,
        config: {
            tag_propagation: {
                enabled: true,
                tag_prefixes: [],
            },
            description_sync: {
                enabled: false,
                table_description_sync_enabled: false,
                column_description_sync_enabled: false,
                container_description_sync_enabled: false,
            },
            databricks: {
                workspace_url: undefined,
                token: undefined,
                warehouse_id: undefined,
            },
        },
    },
};

// Mapping between the UI state values and the recipe config structure
// This is used to enable dynamic updates to the recipe based on custom UI state structures
export const configMap: ConfigMap = {
    ...commonFieldsMapping,
    tagsEnabled: 'action.config.tag_propagation.enabled',
    tags: 'action.config.tag_propagation.tag_prefixes',
    descriptionSyncEnabled: 'action.config.description_sync.enabled',
    tableDescriptionSyncEnabled: 'action.config.description_sync.table_description_sync_enabled',
    columnDescriptionSyncEnabled: 'action.config.description_sync.column_description_sync_enabled',
    containerDescriptionSyncEnabled: 'action.config.description_sync.container_description_sync_enabled',
    'connection.workspace_url': 'action.config.databricks.workspace_url',
    'connection.token': 'action.config.databricks.token',
    'connection.warehouse_id': 'action.config.databricks.warehouse_id',
    // So sad to see this is so confusing!
    propagationAction: {
        isVirtual: true,
        resolveVirtualFormStateFieldValue: (formState: any) => {
            // This is used to create derived formData fields using the recipe values.
            if (
                formState.descriptionSyncEnabled &&
                formState.columnDescriptionSyncEnabled &&
                formState.tableDescriptionSyncEnabled
            ) {
                return 'DESCRIPTIONS';
            }
            if (formState.tagsEnabled) {
                return 'TAGS';
            }
            // No capabilities enabled.
            return undefined;
        },
        onChangeVirtualFormStateFieldValue: (newPropagationAction: any) => {
            // Now map back to the form state we want upon changing the form state.
            switch (newPropagationAction) {
                case 'DESCRIPTIONS': {
                    return {
                        descriptionSyncEnabled: true,
                        tableDescriptionSyncEnabled: true,
                        columnDescriptionSyncEnabled: true,
                        containerDescriptionSyncEnabled: true,
                        tagsEnabled: false,
                    };
                }
                case 'TAGS': {
                    return {
                        descriptionSyncEnabled: false,
                        tableDescriptionSyncEnabled: false,
                        columnDescriptionSyncEnabled: false,
                        containerDescriptionSyncEnabled: false,
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
                            key: 'TAGS',
                            name: 'Tags',
                            description: 'Sync Tags for Tables, Columns, Catalogs, & Schemas (Unity Catalog)',
                        },
                        {
                            key: 'DESCRIPTIONS',
                            name: 'Descriptions',
                            description: 'Sync descriptions for Tables, Columns, Catalogs & Schemas as comments',
                        },
                    ],
                },
            },
        ],
    }),
    getField('select_tags_and_terms', {
        title: 'Select Tags',
        description: 'Choose which tags to sync',
        controlKey: 'propagationAction',
        conditionalKey: 'TAGS',
        fields: [
            {
                props: {
                    fieldTypes: [EntityType.Tag],
                    allowedRadios: ['all', 'some'],
                },
            },
        ],
    }),
    getField('select_connection', {
        title: 'Configure Connection',
        description: 'Provide Databricks connection details to use for metadata sync',
        fields: [
            {
                props: {
                    connectionTypes: ['databricks'],
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
    platform: 'databricks',
    logo: DatabricksLogo,
    name: automationName,
    defaultRecipe,
    description: automationDescription,
    isBeta: true,
    fields,
};
