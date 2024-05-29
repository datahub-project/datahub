import { jsonToYaml } from "../ingest/source/utils";

// Utils for Automations Center 

export enum AutomationStatus {
	ACTIVE = 'active',
	RUNNING = 'running',
	FAILED = 'failed',
	STOPPED = 'stopped',
}

export const titleCase = (input) => {
	return input.split('_')
		.map(word => word.charAt(0).toUpperCase() + word.slice(1))
		.join(' ');
}

export const truncateString = (str, maxLength) => {
	if (str.length > maxLength) return `${str.substring(0, maxLength)}…`;
	return str;
}

export const simplifyDataForListView = (data: any) => {
	return data.map((item: any) => {
		return {
			key: item.urn || titleCase(item.details?.name),
			urn: item.urn,
			name: item.name || titleCase(item.details?.name),
			description: item.description,
			category: item.category || 'Propagation',
			type: item.__typename,
			status: data.status || 'ACTIVE',
			definition: item.definition || item.details,
			updated: new Date(),
			created: new Date(),
		};
	});
}

// Fill YAML with form data
export const getYaml = (automation: any) => {
	if (!automation) return '';

	// const automationType = automation?.type;

	const baseRecipe = automation?.baseRecipe;

	// if (automationType === 'actionPipeline') {
	// 	baseRecipe.name = formData.details.name || "";
	// 	if (baseRecipe.action && baseRecipe.action.config.term_propagation) {
	// 		baseRecipe.action.config.term_propagation.target_terms = formData.termsSelected || "[]";
	// 		baseRecipe.action.config.snowflake.password = ""; // redact password
	// 	}
	// }

	const json = JSON.stringify(baseRecipe, null, 2);

	return jsonToYaml(json);
};