/* 
	This file is a recipe for the Column Documentation Propagation automation.
	It is used to propagate descriptions to downstream columns automatically.

	Action: datahub-integrations-service/src/datahub_integrations/propagation/doc/doc_propagation_action.py
*/

import AcrylLogo from '@images/acryl-logo.svg';
import { AutomationTypes, commonFieldsMapping } from '@app/automations/constants';
import { getField } from '@app/automations/fields';

// Common unique ID for the action
// Used to identify the action in the backend & provide common key between template <> recipe
export const actionType = 'datahub_integrations.propagation.doc.doc_propagation_action.DocPropagationAction';

// Configuration structure for the integration recipe
// Default values can be set here and will be used to populate the UI form
// This is only the information in action.config in the recipe
export const defaultConfig = {};

// Config type export (provides stricture typing)
export type ConfigFields = typeof defaultConfig;

// Mapping between the UI state values and the recipe config structure
// This is used to enable dynamic updates to the recipe based on custom UI state structures
export const configMap: Record<string, string> = {
    ...commonFieldsMapping,
};

// Recipe that's sent in JSON format to the integration service to create or update an automation
// This structure has to match what's expected in the action recipe
export const integrationRecipe = {
    name: 'Column Documentation Propagation',
    description: 'Propagate descriptions to downstream columns automatically',
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
    logo: AcrylLogo,
    type: AutomationTypes.ACTION,
    baseRecipe: integrationRecipe,
    name: integrationRecipe.name,
    description: integrationRecipe.description,
    isDisabled: false,
    isBeta: true,
    fields,
};
