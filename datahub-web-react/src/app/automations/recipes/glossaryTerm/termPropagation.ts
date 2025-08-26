/*
	This file is a recipe for the Glossary Term Propagation automation.
	It is used to propagate Glossary Terms to downstream assets and columns automatically (datasets only).

	Action: datahub-integrations-service/src/datahub_integrations/propagation/term/term_propagation_action.py
*/
import { commonFieldsMapping } from '@app/automations/constants';
import { getField } from '@app/automations/fields';
import { AutomationRecipe, AutomationTemplate } from '@app/automations/types';
import { EntityType } from '@src/types.generated';

import AcrylLogo from '@images/acryl-logo.svg';

// Common unique ID for the action
// Used to identify the action in the backend & provide common key between template <> recipe
export const automationType = 'datahub_integrations.propagation.term.term_propagation_action.TermPropagationAction';

const automationName = 'Glossary Term Propagation';
const automationDescription = 'Propagate Glossary Terms to downstream assets and columns automatically (datasets only)';

// Important: This is the form state which is taken by default, when creating a new automation of this type.
const defaultRecipe: AutomationRecipe = {
    name: automationName,
    description: automationDescription,
    category: 'Data Discovery',
    action: {
        type: automationType,
        config: {
            enabled: true,
            target_terms: [],
            term_groups: [],
        },
    },
};

// Mapping between the UI state values and the recipe config structure
// This is used to enable dynamic updates to the recipe based on custom UI state structures
export const configMap: Record<string, string> = {
    ...commonFieldsMapping,
    termsEnabled: 'action.config.enabled',
    terms: 'action.config.target_terms',
    nodes: 'action.config.term_groups',
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
    isBeta: true,
    fields,
};
