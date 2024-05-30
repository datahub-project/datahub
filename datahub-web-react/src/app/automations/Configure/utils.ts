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

// Define some steps that automations can include
const steps = {
	choose_terms: {
		isHidden: false,
		title: 'Select Terms',
		description: 'Choose the terms that you want to propagate.',
		hasMapping: false,
		canPreview: false,
		previewTitle: undefined,
		fields: [
			{
				type: 'termSelector',
				isRequired: true,
			},
		],
	},
	select_source: {
		isHidden: true, // TODO: Build out custom field & activate
		title: 'Select Source Assets',
		description: 'Choose the source assets (columns or tables) that need to be watched for applications of these terms.',
		canPreview: true,
		previewTitle: 'Preview Source Set',
		fields: [
			{
				type: 'dataAssetSelector',
				isRequired: true,
			},
		],
	},
	select_data_assets: {
		isHidden: false,
		title: 'Select Asset Types',
		description: 'Which asset types should be considered for this automation?',
		canPreview: false,
		fields: [
			{
				type: 'dataAssetSelector',
				isRequired: true,
			},
		],
	},
	select_conditions: {
		isHidden: false,
		title: 'Define Conditions',
		description: 'What criteria must each selected asset type meet?',
		tooltip: 'If you do not provide any conditions, all assets in the selection criteria will be considered passing.',
		canTest: true,
		previewTitle: 'Test Conditions',
		fields: [
			{
				type: 'conditionSelector',
			},
		],
	},
	select_custom_actions: {
		isHidden: false,
		title: 'Add Custom Actions',
		description: 'What actions would you like to apply to the data assets that pass or fail the conditions?',
		fields: [
			{
				type: 'customActionSelector',
			},
		],
	},
	select_traversal: {
		isHidden: false, // TODO: Create builder for these field types & enable
		title: 'Select Traversal',
		description: 'Configure propagation traversal.',
		canPreview: true,
		previewTitle: 'Preview Impacted Set',
		fields: [
			{
				type: 'traversalSelector',
				isRequired: true,
			},
		],
	},
	select_destination: {
		isHidden: false, // TODO: Create builder for these field types & enable
		title: 'Select Destination Connection',
		description: 'Choose the destination connection where the terms will be propagated.',
		canTest: true,
		testTitle: 'Test Connection',
		canPreview: true,
		previewTitle: 'Run on 1 asset',
		connectionTypes: ['snowflake'],
		fields: [
			{
				type: 'connectionSelector',
				isRequired: true,
			},
		],
	},
	details: {
		isHidden: false,
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
			{ ...steps.choose_terms, hasMapping: true },
			{ ...steps.select_source },
			{ ...steps.select_destination },
			{ ...steps.details },
		],
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
			{ ...steps.choose_terms },
			{ ...steps.select_source },
			{ ...steps.select_traversal },
			{ ...steps.details },
		],
		baseRecipe: termPropagation as any,
		isDisabled: false,
	},
	{
		key: 'documentation_propagation',
		type: AutomationTypes.ACTION,
		name: 'Documentation Propagation',
		description: 'This automation propgation column level documentation.',
		logo: AcrylLogo,
		steps: [
			{ ...steps.choose_terms },
			{ ...steps.select_data_assets },
			{ ...steps.select_conditions },
			{ ...steps.details },
		],
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
		baseRecipe: custom as any,
		isDisabled: false,
	},
];

// Get the steps for the automation type
export const getSteps = (key) =>
	selectableAutomations
		.filter((automation) => automation.key === key)[0]?.steps
		.filter((step) => !step.isHidden) || undefined;

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
