import type { Field } from '@app/automations/types';
import { EntityType } from '@src/types.generated';

import { DEFAULT_AUTOMATION_CATEGORY, DEFAULT_APPLY_TYPE, DEFAULT_CARDINALITY, DEFAULT_MODE } from '../constants';

// Specific field components
import { TermSelector, TermSelectorStateType } from './TermSelector';
import { EntityTypeSelector, EntityTypeSelectorStateType } from './EntityTypeSelector';
import { TraversalSelector, TraversalSelectorStateType } from './TraversalSelector';
import { ConnectionSelector, ConnectionSelectorStateType } from './ConnectionSelector';
import { ApplyTypeSelector, ApplyTypeSelectorStateType } from './ApplyTypeSelector';
import { CardinalitySelector, CardinalitySelectorStateType } from './CardinalitySelector';
import { PlatformSelector, PlatformSelectorStateType } from './PlatformSelector';
import { ContainerSelector, ContainerSelectorStateType } from './ContainerSelector';
import { ModeSelector, ModeSelectorStateType } from './ModeSelector';
import { Details, DetailsStateType } from './Details';

// Generic field components
import { RadioSelector } from './RadioSelector';
import { HiddenRecipeModifer } from './HiddenRecipeModifer';
import { PropagationOptions, PropagationOptionsStateType } from './PropagationOptions/PropagationOptions';

// Recipe Modifier (for hidden config fields)
// This field is used to modify the recipe state, usually during a
// conditionally selected option that doesn't have config fields
const hiddenRecipeModifier: Field = {
    title: 'Hidden Recipe Modifier',
    description: 'Modify the recipe state.',
    isHidden: true,
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/HiddenRecipeModifer
            component: HiddenRecipeModifer,

            // Available Component Props to customize the component
            // You can set default values for the props here
            props: {},

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {},
        },
    ],
};

// Generic Radio Selector
const radioSelector: Field = {
    title: 'Select Radio',
    description: 'Choose the radio option.',
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/RadioSelector
            component: RadioSelector,

            // Available Component Props to customize the component
            // You can set default values for the props here
            props: {
                options: [
                    {
                        key: 'option1',
                        name: 'Option 1',
                        description: 'This is option 1',
                    },
                    {
                        key: 'option2',
                        name: 'Option 2',
                        description: 'This is option 2',
                    },
                ],
            },

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                selectedKey: 'option1',
            },
        },
    ],
};

// Term Selector
// This field allows the user to select tags, glossary terms, or glossary nodes
const termSelector: Field = {
    title: 'Select Tags & Terms',
    description: 'Tags and glossary terms to propagate.',
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/TermSelector
            component: TermSelector,

            // Available Component Props to customize the component
            // You can set default values for the props here
            props: {
                fieldTypes: [EntityType.Tag, EntityType.GlossaryTerm, EntityType.GlossaryNode],
                allowedRadios: ['all', 'some', 'none'],
            },

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                terms: [],
                nodes: [],
                tags: [],
                termsEnabled: false,
                tagsEnabled: false,
            } as TermSelectorStateType,
        },
    ],
};

// PropagationOptions
// This field allows the user to determine if propagation should occur downstream and/or with siblings
const propagationOptions: Field = {
    title: 'Configure Propagation Options',
    description: 'Determine if tags should propagate to downstream and sibling assets',
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/PropagationOptions
            component: PropagationOptions,

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                includeDownstreams: true,
                includeSiblings: true,
            } as PropagationOptionsStateType,
        },
    ],
};

// Entity Type Selector
// This field allows the user to select entity types
const entityTypeSelector: Field = {
    title: 'Select Entity Type',
    description: 'Source entity types.',
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

// Apply Type Selector
// This field allows the user to select the apply type of the automation
const applyTypeSelector: Field = {
    title: '',
    description: '',
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/ApplyTypeSelector
            component: ApplyTypeSelector,

            // Available Component Props to customize the component
            // You can set default values for the props here
            props: {
                description: "When unchecked, we'll save suggestions directly on the assets without a human review.",
            },

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                applyType: DEFAULT_APPLY_TYPE,
            } as ApplyTypeSelectorStateType,
        },
    ],
};

// Cardinality Selector
// This field allows the user to select the cardinality of the automation
const cardinalitySelector: Field = {
    title: '',
    description: '',
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/CardinalitySelector
            component: CardinalitySelector,

            // Available Component Props to customize the component
            // You can set default values for the props here
            props: {
                description: 'When unchecked, limit to one term suggestion per dataset/column.',
            },

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                cardinality: DEFAULT_CARDINALITY,
            } as CardinalitySelectorStateType,
        },
    ],
};

// Connection Selector
// This field allows the user to select the destination connection of the automation
const connectionSelector: Field = {
    title: 'Destination Connection',
    description: 'Destination connection where the terms will be propagated.',
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/ConnectionSelector
            component: ConnectionSelector,

            // Available Component Props to customize the component
            // You can set default values for the props here
            props: {
                connectionTypes: ['snowflake', 'bigquery'],
            },

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                connection: undefined,
            } as ConnectionSelectorStateType,
        },
    ],
};

// Mode Selector
// This field allows the user to select the mode the automation
const modeSelector: Field = {
    title: 'Select Mode',
    description: 'Choose the propagation mode for this automation.',
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/ModeSelector
            component: ModeSelector,

            // Available Component Props to customize the component
            // You can set default values for the props here
            props: {},

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                mode: DEFAULT_MODE,
            } as ModeSelectorStateType,
        },
    ],
};

// Platform Selector
// This field allows the user to select the data platforms
const platformSelector: Field = {
    title: 'Data Platforms',
    description: 'Data platforms where the terms will be propagated.',
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/PlatformSelector
            component: PlatformSelector,

            // Available Component Props to customize the component
            // You can set default values for the props here
            props: {
                // Turn on container selector in the platform selector
                enableContainerSelection: false,

                // Confirguable platform urns, this is the default list
                platformUrns: [
                    'urn:li:dataPlatform:snowflake',
                    'urn:li:dataPlatform:bigquery',
                    'urn:li:dataPlatform:redshift',
                    'urn:li:dataPlatform:databricks',
                    'urn:li:dataPlatform:hive',
                    'urn:li:dataPlatform:tableau',
                    'urn:li:dataPlatform:looker',
                    'urn:li:dataPlatform:powerbi',
                    'urn:li:dataPlatform:hudi',
                    'urn:li:dataPlatform:kafka',
                ],
            },

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                platforms: [],
                containers: [],
            } as PlatformSelectorStateType,
        },
    ],
};

// Container Selector
// This field allows the user to select the containers
const containerSelector: Field = {
    title: 'Select Containers',
    description: 'Containers where the terms will be propagated.',
    fields: [
        {
            // The component that's rendered for this field
            // Defined in @app/automations/fields/ContainerSelector
            component: ContainerSelector,

            // Available Component Props to customize the component
            // You can set default values for the props here
            props: {},

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                containers: [],
            } as ContainerSelectorStateType,
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
                executor: {
                    label: 'Executor Pool ID',
                    tooltip: 'Optional Remote Executor Pool ID',
                    placeholder: 'default',
                    isHidden: false,
                    isRequired: false,
                },
            },

            // State mapping to connect form data to the component's state
            // You can set default values for the state here
            state: {
                name: '',
                description: '',
                category: DEFAULT_AUTOMATION_CATEGORY,
                executorId: 'default',
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
    // Generic fields
    radio_selector: radioSelector,
    hidden_recipe_modifier: hiddenRecipeModifier,
    // Specific fields
    select_tags_and_terms: termSelector,
    select_propagation_options: propagationOptions,
    select_entity_types: entityTypeSelector,
    select_traversal_types: traversalSelector,
    select_connection: connectionSelector,
    select_apply_type: applyTypeSelector,
    select_cardinality: cardinalitySelector,
    select_platforms: platformSelector,
    select_containers: containerSelector,
    select_mode: modeSelector,
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
