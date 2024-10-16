/* 
	This file is used to configure the glossary term AI automation.
	The glossary term AI automation is used to automatically propagate glossary terms to assets.

	Action: TBD
*/

import AcrylLogo from '@images/acryl-logo.svg';
import { EntityType } from '@src/types.generated';
import { commonFieldsMapping, DEFAULT_APPLY_TYPE, DEFAULT_CARDINALITY } from '@app/automations/constants';
import { getField } from '@app/automations/fields';

// Common unique ID for the action
// Used to identify the action in the backend & provide common key between template <> recipe
export const automationType = 'ai_term_suggestion';

const automationName = 'Glossary Term AI';
const automationDescription = 'Add or propose Glossary Terms to assets and columns using AI';

// Important: This is the form state which is taken by default, when creating a new automation of this type.
const defaultRecipe = {
    name: automationName,
    description: automationDescription,
    category: 'Data Discovery',
    action: {
        type: automationType,
        config: {
            entity_types_enabled: [EntityType.Dataset, EntityType.SchemaField],
            glossary_term_urns: [],
            glossary_node_urns: [],
            recommendation_action: DEFAULT_APPLY_TYPE,
            cardinality: DEFAULT_CARDINALITY,
            platforms: [],
            containers: [],
        },
    },
};

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

// Define UI fields for the create & edit forms
// See implementation docs for field definitions in @app/automations/fields/index
// Pro tip: `getField` allows overriding default component variables
const fields = [
    getField('select_tags_and_terms', {
        title: 'Glossary Terms & Groups',
        description: 'Glossary Terms and Term Groups.',
        fields: [
            {
                props: {
                    fieldTypes: [EntityType.GlossaryTerm, EntityType.GlossaryNode],
                    allowedRadios: ['some'],
                },
            },
        ],
    }),
    getField('select_entity_types', {
        title: 'Asset Types',
        description: 'Types of assets to apply Glossary Terms to.',
        fields: [
            {
                props: {
                    entityTypes: [EntityType.Dataset, EntityType.SchemaField],
                    placeholder: 'Select Datasets, Columns, or both...',
                },
            },
        ],
    }),
    getField('select_apply_type', {
        fields: [],
    }),
    getField('select_cardinality', {
        title: 'Allowed Term Count',
        description: 'When unchecked, limit to one term suggestion per dataset/column.',
        fields: [],
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
        fields: [],
    }),
];

// Template for rendering all the things needed in the UI for creating/editing
// an automation based off a templated recipe system
export const template = {
    key: automationType,
    type: automationType,
    platform: 'acryl',
    name: automationName,
    description: automationDescription,
    defaultRecipe,
    logo: AcrylLogo,
    isDisabled: false,
    isBeta: true,
    fields,
};
