/*
	This file is a recipe for the Glossary Term Propagation V2 automation.
	It is used to propagate glossary terms to upstream and downstream assets and columns automatically (datasets only).

	Action: datahub-integrations-service/src/datahub_integrations/propagation/propagation/generic_propagation_action.py
*/
import { commonFieldsMapping } from '@app/automations/constants';
import { getField } from '@app/automations/fields';
import { GENERIC_PROPAGATION_ACTION, GenericPropagationConfig } from '@app/automations/shared/propagation';
import { AutomationRecipe, AutomationTemplate } from '@app/automations/types';
import { AppConfig, EntityType } from '@src/types.generated';

import DataHubLogo from '@images/acryl-logo.svg';

const automationType = GENERIC_PROPAGATION_ACTION;

const automationName = 'Glossary Term Propagation';
const automationDescription =
    'Propagate Glossary Terms to upstream and / or downstream assets and columns automatically';

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
                metadata_propagated: { terms: { enabled: true } },
            },
        } as GenericPropagationConfig,
    },
};

// Mapping between the UI state values and the recipe config structure
// This is used to enable dynamic updates to the recipe based on custom UI state structures
const configMap: Record<string, string> = {
    ...commonFieldsMapping,
    termsEnabled: 'action.config.propagation_rule.metadata_propagated.terms.enabled',
    terms: 'action.config.propagation_rule.metadata_propagated.terms.target_terms',
    nodes: 'action.config.propagation_rule.metadata_propagated.terms.term_groups',
    targetUrnResolution: 'action.config.propagation_rule.target_urn_resolution',
};

// Define UI fields for the create & edit forms
// See implementation docs for field definitions in @app/automations/fields/index
// Pro tip: `getField` allows overriding default component variables
const fields = [
    getField('select_tags_and_terms', {
        title: 'Glossary Terms & Groups',
        description: 'glossary terms and term groups to propagate.',
        fields: [
            {
                props: {
                    fieldTypes: [EntityType.GlossaryTerm, EntityType.GlossaryNode],
                },
            },
        ],
    }),
    getField('propagation_rule', {
        title: 'Configure Propagation Options',
        description: 'Determine in which directions glossary terms should be propagated.',
        fields: [],
    }),
    getField('details', {
        fields: [],
    }),
];

// Template for rendering all the things needed in the UI for creating/editing
// an automation based off a templated recipe system
const template: AutomationTemplate = {
    key: `${automationType}-terms`, // Must match value set in metadata_propagated
    type: automationType,
    platform: 'acryl',
    logo: DataHubLogo,
    name: automationName,
    description: automationDescription,
    defaultRecipe,
    isDisabled: (appConfig: AppConfig) => !appConfig.featureFlags.termPropagationV2Enabled,
    isBeta: true,
    fields,
    configMap,
};

export default template;
