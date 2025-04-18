/* 
	This file is a recipe for the Column Documentation Propagation automation.
	It is used to propagate descriptions to downstream columns automatically.

	Action: datahub-integrations-service/src/datahub_integrations/propagation/doc/doc_propagation_action.py
*/
import { commonFieldsMapping } from '@app/automations/constants';
import { getField } from '@app/automations/fields';
import { AutomationRecipe, AutomationTemplate, ConfigMap } from '@app/automations/types';

import AcrylLogo from '@images/acryl-logo.svg';

// Common unique ID for the action
// Used to identify the action in the backend & provide common key between template <> recipe
export const automationType = 'datahub_integrations.propagation.doc.doc_propagation_action.DocPropagationAction';

const automationName = 'Column Documentation Propagation';
const automationDescription = 'Propagate descriptions to downstream columns automatically';

// Important: This is the form state which is taken by default, when creating a new automation of this type.
export const defaultRecipe: AutomationRecipe = {
    name: automationName,
    description: automationDescription,
    category: 'Data Discovery',
    action: {
        type: automationType,
        config: {},
    },
};

// Mapping between the UI state values and the recipe config structure
// This is used to enable dynamic updates to the recipe based on custom UI state structures
export const configMap: ConfigMap = {
    ...commonFieldsMapping,
};

// Define UI fields for the create & edit forms
// See implementation docs for field definitions in @app/automations/fields/index
// Pro tip: `getField` allows overriding default component variables
const fields = [
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
