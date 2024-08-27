// Images
import SnowflakeLogo from '@images/snowflakelogo.png';
import AcrylLogo from '@images/acryl-logo.svg';

import { EntityType } from '@src/types.generated';

import { AutomationTypes } from '@app/automations/constants';
import type { AutomationTemplate, Fields } from '@app/automations/types';

// Recipes
import recipes from '@src/app/automations/recipes';

// Define the available fields that can be used in the create/update automation form
const steps: Fields = {
    choose_terms: {
        title: 'Select Tags & Terms',
        description: 'Choose the tags and glossary terms to propagate.',
        fields: [
            {
                type: 'termSelector',
                props: {
                    fieldTypes: [EntityType.Tag, EntityType.GlossaryTerm],
                },
            },
        ],
    },
    select_source: {
        title: 'Select Source Assets',
        description:
            'Choose the source assets (columns or tables) that need to be watched for applications of these terms.',
        fields: [
            {
                type: 'dataAssetSelector',
                isRequired: true,
            },
        ],
    },
    select_data_assets: {
        title: 'Select Asset Types',
        description: 'Which asset types should be considered for this automation?',
        fields: [
            {
                type: 'dataAssetSelector',
                isRequired: true,
            },
        ],
    },
    select_conditions: {
        title: 'Define Conditions',
        description: 'What criteria must each selected asset type meet?',
        tooltip:
            'If you do not provide any conditions, all assets in the selection criteria will be considered passing.',
        fields: [
            {
                type: 'conditionSelector',
            },
        ],
    },
    select_custom_actions: {
        title: 'Add Custom Actions',
        description: 'What actions would you like to apply to the data assets that pass or fail the conditions?',
        fields: [
            {
                type: 'customActionSelector',
            },
        ],
    },
    select_traversal: {
        title: 'Select Traversal',
        description: 'Configure propagation traversal.',
        fields: [
            {
                type: 'traversalSelector',
                isRequired: true,
            },
        ],
    },
    select_destination: {
        title: 'Select Destination Connection',
        description: 'Choose the destination connection where the terms will be propagated.',
        fields: [
            {
                type: 'connectionSelector',
                props: {
                    connectionTypes: ['snowflake'],
                },
                isRequired: true,
            },
        ],
    },
    details: {
        title: 'Configure Details',
        description: 'Provide a name and description for this automation.',
        fields: [
            {
                type: 'text',
                label: 'Name',
                isRequired: true,
            },
            {
                type: 'longtext',
                label: 'Description',
            },
            {
                type: 'categorySelector',
                label: 'Category',
                isRequied: true,
            },
        ],
    },
};

// Define the available automation templates that can be used to create a new automation
export const automationTemplates: AutomationTemplate[] = [
    {
        key: 'snowflake_tag_propagation',
        platform: 'snowflake',
        type: AutomationTypes.ACTION,
        name: 'Snowflake Tag Propagation',
        description: 'Sync Tags and Glossary Terms to Snowflake Table and Column Tags',
        logo: SnowflakeLogo,
        fields: [
            {
                ...steps.choose_terms,
                description: 'Choose the tags and glossary terms to propagate to Snowflake.',
            },
            { ...steps.select_destination },
            { ...steps.details },
        ],
        requiredFields: ['name', 'terms', 'connection'],
        baseRecipe: recipes.snowflakeTagPropagation as any,
        isDisabled: false,
    },
    {
        key: 'term_propagation',
        platform: 'acryl',
        type: AutomationTypes.ACTION,
        name: 'Glossary Term Propagation',
        description: 'Propagate Glossary Terms to downstream assets and columns automatically (datasets only)',
        logo: AcrylLogo,
        fields: [
            {
                ...steps.choose_terms,
                title: 'Select Glossary Terms & Groups',
                description: 'Choose the glossary terms and term groups to propagate.',
                fields: [
                    {
                        ...steps.choose_terms.fields[0],
                        props: {
                            fieldTypes: [EntityType.GlossaryTerm, EntityType.GlossaryNode],
                        },
                    },
                ],
            },
            { ...steps.details },
        ],
        requiredFields: ['name'],
        baseRecipe: recipes.termPropagation as any,
        isDisabled: false,
    },
    {
        key: 'column_documentation_propagation',
        platform: 'acryl',
        type: AutomationTypes.ACTION,
        name: 'Column Documentation Propagation',
        description: 'Propagate descriptions to downstream columns automatically',
        logo: AcrylLogo,
        fields: [{ ...steps.details }],
        requiredFields: ['name'],
        baseRecipe: recipes.columnLevelDocPropagation as any,
        isDisabled: false,
    },
    // This completely threw exception on click. Hiding since we have metadata tests still.
    // {
    //     key: 'custom',
    //     platform: 'acryl',
    //     type: AutomationTypes.TEST,
    //     name: 'Custom Automation',
    //     description: 'This automation allows you create a metdata test.',
    //     logo: AcrylLogo,
    //     fields: [
    //         { ...steps.select_data_assets },
    //         { ...steps.select_conditions },
    //         { ...steps.select_custom_actions },
    //         { ...steps.details },
    //     ],
    //     requiredFields: ['name'],
    //     baseRecipe: recipes.custom as any,
    //     isDisabled: hideMetadataTests,
    // },
];

// Return the available automation templates
export const useGetAutomationTemplates = () => {
    return automationTemplates;
};
