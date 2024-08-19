export default {
    filter: {
        event_type: 'EntityChangeEvent_v1',
    },
    action: {
        type: 'datahub_integrations.propagation.snowflake.tag_propagator.SnowflakeTagPropagatorAction',
        config: {
            term_propagation: {
                enabled: true,
                target_terms: [],
            },
            tag_propagation: {
                enabled: true,
                tag_prefixes: [],
            },
            snowflake: {
                account_id: '',
                warehouse: '',
                username: '',
                password: '',
                role: '',
                database: '',
                schema: '',
            },
        },
    },
};
