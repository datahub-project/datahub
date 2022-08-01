import { SourceConfig } from '../types';
import kafkaLogo from '../../../../../images/kafkalogo.png';

const placeholderRecipe = `\
source:
    type: kafka
    config:
        # Replace with your cluster ID
        platform_instance: "YOUR_CLUSTER_ID"
        connection:
            bootstrap: # Your Kafka bootstrap host, e.g. "broker:9092"

            # Uncomment and add secrets in Secrets Tab
            # consumer_config:
            #     security.protocol: "SASL_SSL"
            #     sasl.mechanism: "PLAIN"
            #     sasl.username: "\${CLUSTER_API_KEY_ID}"
            #     sasl.password: "\${CLUSTER_API_KEY_SECRET}"

            schema_registry_url: # Your Kafka Schema Registry url, e.g. http://schemaregistry:8081
            # Uncomment and add secrets in Secrets Tab
            # schema_registry_config:
            #     basic.auth.user.info: "\${REGISTRY_API_KEY_ID}:\${REGISTRY_API_KEY_SECRET}"
`;

const kafkaConfig: SourceConfig = {
    type: 'kafka',
    placeholderRecipe,
    displayName: 'Kafka',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/kafka/',
    logoUrl: kafkaLogo,
};

export default kafkaConfig;
