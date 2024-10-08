/* 
	This file is used to configure the glossary term AI automation.
	The glossary term AI automation is used to automatically propagate glossary terms to assets.

	Action: TBD
*/

import AcrylLogo from '@images/acryl-logo.svg';
import { EntityType } from '@src/types.generated';
import {
    AutomationTypes,
    commonFieldsMapping,
    DEFAULT_APPLY_TYPE,
    DEFAULT_CARDINALITY,
} from '@app/automations/constants';
import { getField } from '@app/automations/fields';

// Common unique ID for the action
// Used to identify the action in the backend & provide common key between template <> recipe
export const actionType = 'ai_term_suggestion';

// Configuration structure for the integration recipe
// Default values can be set here and will be used to populate the UI form
// This is only the information in action.config in the recipe
export const defaultConfig = {
    entity_types_enabled: [EntityType.Dataset, EntityType.SchemaField],
    glossary_term_urns: [],
    glossary_node_urns: [],
    recommendation_action: DEFAULT_APPLY_TYPE,
    cardinality: DEFAULT_CARDINALITY,
    platforms: [],
    containers: [],
};

// Config type export (provides strict typing)
export type ConfigFields = typeof defaultConfig;

// Mapping between the UI state values and the recipe config structure
// This is used to enable dynamic updates to the recipe based on custom UI state structures
export const configMap: Record<string, string> = {
    ...commonFieldsMapping,
    entities: 'action.config.entity_types_enabled',
    terms: 'action.config.glossary_term_urns',
    nodes: 'action.config.glossary_node_urns',
    applyType: 'action.config.recommendation_action',
    cardinality: 'action.config.cardinality',
    platforms: 'action.config.platforms',
    containers: 'action.config.containers',
};

// Recipe that's sent in JSON format to the integration service to create or update an automation
// This structure has to match what's expected in the action recipe
export const integrationRecipe = {
    name: 'Glossary Term AI',
    description: 'Add or propose Glossary Terms to assets and columns using AI',
    executorId: 'default',
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
        title: 'Select Glossary Terms & Groups',
        description: 'Choose the Glossary Terms and Term Groups.',
        fields: [
            {
                props: {
                    fieldTypes: [EntityType.GlossaryTerm, EntityType.GlossaryNode],
                    allowedRadios: ['some'],
                },
                state: {
                    termsEnabled: true,
                    terms: [],
                    nodes: [],
                },
            },
        ],
    }),
    getField('select_entity_types', {
        title: 'Asset Types',
        description: 'Choose the types of assets to apply Glossary Terms to.',
        fields: [
            {
                props: {
                    entityTypes: [EntityType.Dataset, EntityType.SchemaField],
                    placeholder: 'Select Datasets, Columns, or both...',
                },
                state: {
                    entities: defaultConfig.entity_types_enabled,
                },
            },
        ],
    }),
    getField('select_apply_type', {
        fields: [
            {
                state: {
                    applyType: defaultConfig.recommendation_action,
                },
            },
        ],
    }),
    getField('select_cardinality', {
        title: 'Allowed Term Count',
        description: 'Select the maximum number of terms to recommend.',
        fields: [
            {
                state: {
                    cardinality: defaultConfig.cardinality,
                },
            },
        ],
    }),
    getField('select_platforms', {
        fields: [
            {
                props: {
                    enableContainerSelection: true,
                },
            },
        ],
    }),
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
    platform: 'acryl',
    type: AutomationTypes.ACTION,
    name: integrationRecipe.name,
    description: integrationRecipe.description,
    logo: AcrylLogo,
    baseRecipe: integrationRecipe,
    isDisabled: false,
    isBeta: true,
    fields,
};
