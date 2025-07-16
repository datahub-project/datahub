/*
	This file is a recipe for the Tag Propagation V2 automation.
	It is used to propagate tags to upstream and downstream assets and columns automatically (datasets only).

	Action: datahub-integrations-service/src/datahub_integrations/propagation/propagation/generic_propagation_action.py
*/
import { commonFieldsMapping } from '@app/automations/constants';
import { getField } from '@app/automations/fields';
import { PropagationOptionsV2 } from '@app/automations/fields/PropagationOptions/PropagationOptionsV2';
import { AutomationRecipe, AutomationTemplate } from '@app/automations/types';
import { AppConfig, EntityType } from '@src/types.generated';

import AcrylLogo from '@images/acryl-logo.svg';

// Common unique ID for the action
// Used to identify the action in the backend & provide common key between template <> recipe
export const automationType =
    'datahub_integrations.propagation.propagation.generic_propagation_action.GenericPropagationAction';

const automationName = 'Tag Propagation';
const automationDescription = 'Propagate Tags to upstream and/or downstream assets and columns automatically';

// Important: This is the form state which is taken by default, when creating a new automation of this type.
const defaultRecipe: AutomationRecipe = {
    name: automationName,
    description: automationDescription,
    category: 'Data Discovery',
    action: {
        type: automationType,
        config: {
            propagation_rule: {
                entityTypes: ['dataset', 'schemaField'],
                targetUrnResolution: [{ type: 'downstream' }, { type: 'upstream' }],
                metadataPropagated: {
                    tags: {
                        enabled: true,
                    },
                },
            },
        },
    },
};

// Mapping between the UI state values and the recipe config structure
// This is used to enable dynamic updates to the recipe based on custom UI state structures
export const configMap: Record<string, string> = {
    ...commonFieldsMapping,
    tagsEnabled: 'action.config.propagation_rule.metadataPropagated.tags.enabled',
    tags: 'action.config.propagation_rule.metadataPropagated.tags.tag_prefixes',
    entityTypes: 'action.config.propagation_rule.entityTypes',
    targetUrnResolution: 'action.config.propagation_rule.targetUrnResolution',
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
    getField('select_propagation_options_v2', {
        title: 'Configure Propagation Options',
        description: 'Determine if tags should propagate to upstream and/or downstream assets',
        fields: [
            {
                component: PropagationOptionsV2,
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
    platform: 'acryl',
    logo: AcrylLogo,
    name: automationName,
    description: automationDescription,
    defaultRecipe,
    isDisabled: (appConfig: AppConfig) => !appConfig.featureFlags.tagPropagationV2Enabled,
    isBeta: true,
    fields,
};
