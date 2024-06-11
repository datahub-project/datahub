import { env } from '../constants';

import { TEST_CATEGORY_NAME_TO_INFO } from './constants';

import {
	snowflakeTagPropagation,
	termPropagation,
	documentationPropagation,
	custom
} from './recipes';

import { AutomationTypes } from '../utils';

// Images 
import SnowflakeLogo from '../../../images/snowflakelogo.png';
import AcrylLogo from '../../../images/acryl-logo.svg';

export type Field = {
	type: string;
	isRequired: boolean;
};

export type Step = {
	title: string;
	description: string;
	fields: any[];
	tooltip?: string;
	config?: any;
};

export type Steps = Record<string, Step>;

const { hideMetadataTests } = env;

// Define some steps that automations can include
const steps: Steps = {
	choose_terms: {
		title: 'Select Terms',
		description: 'Choose the terms that you want to propagate.',
		fields: [
			{
				type: 'termSelector',
				isRequired: true,
			},
		],
	},
	select_source: {
		title: 'Select Source Assets',
		description: 'Choose the source assets (columns or tables) that need to be watched for applications of these terms.',
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
		tooltip: 'If you do not provide any conditions, all assets in the selection criteria will be considered passing.',
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
		config: {
			connectionTypes: ['snowflake'],
		},
		fields: [
			{
				type: 'connectionSelector',
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

// Define the automations that are available for selection
export const selectableAutomations = [
	{
		key: 'snowflake_tag_propagation',
		type: AutomationTypes.ACTION,
		name: 'Snowflake Tag Propagation',
		description: 'This automation allows you to propagate tags from one Snowflake table to another.',
		logo: SnowflakeLogo,
		steps: [
			{ ...steps.choose_terms },
			// { ...steps.select_source },
			// { ...steps.select_conditions },
			{ ...steps.select_destination },
			{ ...steps.details },
		],
		requiredFields: ['name', 'terms', 'connection'],
		baseRecipe: snowflakeTagPropagation as any,
		isDisabled: false,
	},
	{
		key: 'term_propagation',
		type: AutomationTypes.ACTION,
		name: 'Term Propagation',
		description: 'This automation allows you to propagate terms via lineage.',
		logo: AcrylLogo,
		steps: [
			// { ...steps.choose_terms },
			// { ...steps.select_source },
			// { ...steps.select_traversal },
			{ ...steps.details },
		],
		requiredFields: ['name'],
		baseRecipe: termPropagation as any,
		isDisabled: false,
	},
	{
		key: 'documentation_propagation',
		type: AutomationTypes.ACTION,
		name: 'Documentation Propagation',
		description: 'This automation propagates column level documentation.',
		logo: AcrylLogo,
		steps: [
			// { ...steps.choose_terms },
			// { ...steps.select_data_assets },
			// { ...steps.select_conditions },
			{ ...steps.details },
		],
		requiredFields: ['name'],
		baseRecipe: documentationPropagation as any,
		isDisabled: false,
	},
	{
		key: 'custom',
		type: AutomationTypes.TEST,
		name: 'Custom Automation',
		description: 'This automation allows you create a metdata test.',
		logo: AcrylLogo,
		steps: [
			{ ...steps.select_data_assets },
			{ ...steps.select_conditions },
			{ ...steps.select_custom_actions },
			{ ...steps.details },
		],
		requiredFields: ['name'],
		baseRecipe: custom as any,
		isDisabled: hideMetadataTests,
	},
];

// Get the steps for the automation type
export const getSteps = (key) =>
	selectableAutomations
		.filter((automation) => automation.key === key)[0]?.steps;

// Get the data of the automation type
export const getAutomationData = (key, type) => {
	const automation = selectableAutomations.filter((auto) => {
		return auto.key === key || auto.baseRecipe?.action?.type === type
	});
	return automation ? automation[0] : undefined;
}

// Returns true if the category name is "well-supported" (e.g. a built in), false otherwise.
export const isSupportedCategory = (categoryName) => {
	return TEST_CATEGORY_NAME_TO_INFO.get(categoryName) !== undefined;
};