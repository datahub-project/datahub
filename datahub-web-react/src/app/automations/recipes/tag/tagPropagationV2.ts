/*
	This file is a recipe for the Tag Propagation V2 automation.
	It is used to propagate tags to upstream and downstream assets and columns automatically (datasets only).

	Action: datahub-integrations-service/src/datahub_integrations/propagation/propagation/generic_propagation_action.py
*/
import { commonFieldsMapping } from '@app/automations/constants';
import { getField } from '@app/automations/fields';
import { GENERIC_PROPAGATION_ACTION, GenericPropagationConfig } from '@app/automations/shared/propagation';
import { AutomationRecipe, AutomationTemplate } from '@app/automations/types';
import { AppConfig, EntityType } from '@src/types.generated';

import DataHubLogo from '@images/acryl-logo.svg';

// Common unique ID for the action
// Used to identify the action in the backend & provide common key between template <> recipe
const automationType = GENERIC_PROPAGATION_ACTION;

const automationName = 'Tag Propagation';
const automationDescription =
    'Propagate Tags to upstream and / or downstream assets and columns automatically (datasets only)';

// Important: This is the form state which is taken by default, when creating a new automation of this type.
const defaultRecipe: AutomationRecipe = {
    name: automationName,
    description: automationDescription,
    category: 'Data Discovery',
    action: {
        type: automationType,
        config: {
            propagation_rule: {
                entity_types: ['dataset', 'schemaField'],
                target_urn_resolution: [
                    { lookup_type: 'relationship', type: 'downstream' },
                    { lookup_type: 'relationship', type: 'upstream' },
                ],
                metadata_propagated: { tags: { enabled: true } },
            },
        } as GenericPropagationConfig,
    },
};

// Mapping between the UI state values and the recipe config structure
// This is used to enable dynamic updates to the recipe based on custom UI state structures
const configMap: Record<string, string> = {
    ...commonFieldsMapping,
    tagsEnabled: 'action.config.propagation_rule.metadata_propagated.tags.enabled',
    tags: 'action.config.propagation_rule.metadata_propagated.tags.tag_prefixes',
    targetUrnResolution: 'action.config.propagation_rule.target_urn_resolution',
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
    getField('propagation_rule', {
        title: 'Configure Propagation Options',
        description: 'Determine in which directions tags should be propagated.',
        fields: [],
    }),
    getField('details', {
        fields: [],
    }),
];

// Template for rendering all the things needed in the UI for creating/editing
// an automation based off a templated recipe system
const template: AutomationTemplate = {
    key: `${automationType}-tags`, // Must match value set in metadata_propagated
    type: automationType,
    platform: 'acryl',
    logo: DataHubLogo,
    name: automationName,
    description: automationDescription,
    defaultRecipe,
    isDisabled: (appConfig: AppConfig) => !appConfig.featureFlags.tagPropagationV2Enabled,
    isBeta: true,
    fields,
    configMap,
};

export default template;
