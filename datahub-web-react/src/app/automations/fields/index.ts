import type { Field } from '@app/automations/types';
import { EntityType } from '@src/types.generated';

import { DEFAULT_AUTOMATION_CATEGORY } from '../constants';

import { TermSelector, TermSelectorStateType } from './TermSelector';
import { EntityTypeSelector, EntityTypeSelectorStateType } from './EntityTypeSelector';
import { TraversalSelector, TraversalSelectorStateType } from './TraversalSelector';
import { ConnectionSelector, ConnectionSelectorStateType } from './ConnectionSelector';
import { Details, DetailsStateType } from './Details';

// Term Selector
// This field allows the user to select tags, glossary terms, or glossary nodes
const termSelector: Field = {
    title: 'Select Tags & Terms',
    description: 'Choose the tags and glossary terms to propagate.',
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/TermSelector
            component: TermSelector,

            // Available Component Props to customize the component
            // You can set default values for the props here
            props: {
                fieldTypes: [EntityType.Tag, EntityType.GlossaryTerm, EntityType.GlossaryNode],
            },

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                terms: [],
                nodes: [],
                tags: [],
                termsEnabled: true,
                tagsEnabled: true,
            } as TermSelectorStateType,
        },
    ],
};

// Entity Type Selector
// This field allows the user to select entity types
const entityTypeSelector: Field = {
    title: 'Select Entity Type',
    description: 'Choose the source entity types.',
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/EntityTypeSelector
            component: EntityTypeSelector,

            // Available Component Props to customize the component
            // You can set default values for the props here
            props: {
                entityTypes: [], // this will render all types in registry by default
            },

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                entities: [],
            } as EntityTypeSelectorStateType,
        },
    ],
};

// Traversal Selector
// This field allows the user to select a traversal configuration
const traversalSelector: Field = {
    title: 'Select Traversal',
    description: 'Configure propagation traversal.',
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/TraversalSelector
            component: TraversalSelector,

            // Available Component Props to customize the component
            // You can set default values for the props here
            props: {},

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                lineage: [],
                hierarchy: [],
            } as TraversalSelectorStateType,
        },
    ],
};

// Connection Selector
// This field allows the user to select the destination connection of the automation
const connectionSelector: Field = {
    title: 'Select Destination Connection',
    description: 'Choose the destination connection where the terms will be propagated.',
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/ConnectionSelector
            component: ConnectionSelector,

            // Available Component Props to customize the component
            // You can set default values for the props here
            props: {
                connectionTypes: ['snowflake'],
            },

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                connection: undefined,
            } as ConnectionSelectorStateType,
        },
    ],
};

// Detail Fields
// This field allows the user to provide a name, description, and category for the automation
const details = {
    title: 'Configure Details',
    description: 'Provide a name, description, and category for this automation.',
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/Details
            component: Details,

            // Component default props
            // You can set default values for the props here
            props: {
                name: {
                    label: 'Name',
                    placeholder: 'Enter a name for the automation',
                    isRequired: true,
                },
                description: {
                    label: 'Description',
                    placeholder: 'Enter a description for the automation',
                    isRequired: false,
                },
                category: {
                    label: 'Category',
                    placeholder: 'Enter a category for the automation',
                    isHidden: false, // ability to hide the category field
                    isRequired: true,
                },
            },

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                name: undefined,
                description: undefined,
                category: DEFAULT_AUTOMATION_CATEGORY,
            } as DetailsStateType,
        },
    ],
};

// // Condition Selector
// // This field allows the user to define conditions for the selected asset types
// const conditionSelector: Field = {
//     title: 'Define Conditions',
//     description: 'What criteria must each selected asset type meet?',
//     tooltip: 'If you do not provide any conditions, all assets in the selection criteria will be considered passing.',
//     fields: [
//         {
//             // Component defined in @app/automations/fields/ConditionSelector
//             type: 'conditionSelector',

//             // Component default props
//             props: {},
//         },
//     ],
// };

// // Custom Action Selector
// // This field allows the user to add custom actions to the data assets that pass or fail the conditions
// const customActionSelector: Field = {
//     title: 'Add Custom Actions',
//     description: 'What actions would you like to apply to the data assets that pass or fail the conditions?',
//     fields: [
//         {
//             // Component defined in @app/automations/fields/CustomActionSelector
//             type: 'customActionSelector',

//             // Component default props
//             props: {},
//         },
//     ],
// };

// Define the available fields that can be used in the create/update automation form
const fields = {
    select_tags_and_terms: termSelector,
    select_entity_types: entityTypeSelector,
    select_traversal_types: traversalSelector,
    select_connection: connectionSelector,
    details,
    // select_conditions: conditionSelector,
    // select_custom_actions: customActionSelector,
};

// Define the available fields that can be used in the create/update automation form
export type AvailableFields = typeof fields;

// Function to get a field and customize it's properties such as title, description, and default props
// It returns the field with the updated properties
function getField(fieldName: keyof AvailableFields, customizations: Partial<Field> = {}): Field {
    const field = fields[fieldName];
    if (!field) throw new Error(`Field "${fieldName}" not found.`);
    const updatedField: Field = {
        ...field,
        ...customizations,
        fields: field.fields.map((f, index) => ({
            ...f,
            ...customizations.fields?.[index],
            props: {
                ...f.props,
                ...(customizations.fields?.[index]?.props || {}),
            },
        })),
    };
    return updatedField;
}

// Export the fields object and the getField function
export { fields, getField };
