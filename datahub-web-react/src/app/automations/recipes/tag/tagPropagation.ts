/*
	This file is a recipe for the Tag Propagation automation.
	It is used to propagate Tags to downstream assets and columns automatically (datasets only).

	Action: datahub-integrations-service/src/datahub_integrations/propagation/tag/tag_propagation_action.py
*/

import AcrylLogo from '@images/acryl-logo.svg';
import { commonFieldsMapping } from '@app/automations/constants';
import { getField } from '@app/automations/fields';
import { EntityType } from '@src/types.generated';

// Common unique ID for the action
// Used to identify the action in the backend & provide common key between template <> recipe
export const automationType = 'datahub_integrations.propagation.tag.tag_propagation_action.TagPropagationAction';

const automationName = 'Tag Propagation';
const automationDescription = 'Propagate Tags to downstream assets and columns automatically (datasets only)';

// Important: This is the form state which is taken by default, when creating a new automation of this type.
const defaultRecipe = {
    name: automationName,
    description: automationDescription,
    category: 'Data Discovery',
    action: {
        type: automationType,
        config: {
            enabled: true,
            tag_prefixes: [],
            include_downstreams: true,
            include_siblings: true,
        },
    },
};

// Mapping between the UI state values and the recipe config structure
// This is used to enable dynamic updates to the recipe based on custom UI state structures
export const configMap: Record<string, string> = {
    ...commonFieldsMapping,
    tagsEnabled: 'action.config.enabled',
    tags: 'action.config.tag_prefixes',
    includeDownstreams: 'action.config.include_downstreams',
    includeSiblings: 'action.config.include_siblings',
};

// Define UI fields for the create & edit forms
// See implementation docs for field definitions in @app/automations/fields/index
// Pro tip: `getField` allows overriding default component variables
const fields = [
    getField('select_tags_and_terms', {
        title: 'Select Tags',
        description: 'Choose the tags to propagate to other assets.',
        fields: [
            {
                props: {
                    fieldTypes: [EntityType.Tag],
                },
                state: {
                    tags: [],
                    tagsEnabled: true,
                },
            },
        ],
    }),
    getField('select_propagation_options', {
        title: 'Configure Propagation Options',
        description: 'Determine if tags should propagate to downstream and/or sibling assets',
        fields: [],
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
    logo: AcrylLogo,
    name: automationName,
    description: automationDescription,
    defaultRecipe,
    isDisabled: false,
    isBeta: true,
    fields,
};
