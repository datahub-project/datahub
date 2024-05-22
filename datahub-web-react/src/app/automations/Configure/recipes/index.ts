/* eslint-disable no-template-curly-in-string */

const kafkaBootstrap = '${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}';
const schemaRegistryUrl = '${SCHEMA_REGISTRY_URL:-http"://localhost:8080/schema-registry/api/}';
const datahubServerUrl = '${DATAHUB_SERVER_URL:-http://localhost:8080}';

export const snowflakeTagPropagation = {
	name: '', // to be filled by form data
	source: {
		type: 'kafka',
		config: {
			connection: {
				bootstrap: `${kafkaBootstrap}`,
				schema_registry_url: `${schemaRegistryUrl}`
			}
		}
	},
	filter: {
		event_type: 'EntityChangeEvent_v1'
	},
	action: {
		type: 'datahub_integrations.propagation.snowflake.tag_propagator: SnowflakeTagPropagatorAction',
		config: {
			term_propagation: {
				target_terms: [] // to be filled by form data
			},
			snowflake: {
				account_id: 'xaa48144',
				warehouse: 'COMPUTE_WH',
				username: 'swaroop',
				password: 'E*s7oA6mDwNA8Q',
				role: 'ACCOUNTADMIN',
			}
		}
	}
};

export const documentationPropagation = {
	name: '', // to be filled by form data
	source: {
		type: 'kafka',
		config: {
			connection: {
				bootstrap: `${kafkaBootstrap}`,
				schema_registry_url: `${schemaRegistryUrl}`
			}
		}
	},
	datahub: {
		server: `${datahubServerUrl}`
	},
	action: {
		type: 'datahub_integrations.propagation.doc.doc_propagation_action.DocPropagationAction',
		config: {}
	}
};

export const termPropagation = {
	name: '', // to be filled by form data
	source: {
		type: 'kafka',
		config: {
			connection: {
				bootstrap: `${kafkaBootstrap}`,
				schema_registry_url: `${schemaRegistryUrl}`
			}
		}
	},
	action: {
		type: 'datahub_integrations.propagation.term.term_propagation_action.TermPropagationAction',
		config: {}
	}
};

export const custom = {
	on: {
		types: [] // to be filled by form data
	},
	rules: [] // to be filled by form data
};