import { SourceConfig } from '../types';
import kafkaLogo from '../../../../../images/kafkalogo.png';

const baseUrl = window.location.origin;

const placeholderRecipe = `\
source:
    type: kafka
    config:
        # Coordinates
        connection:
            bootstrap: # Your Kafka bootstrap host, e.g. "broker:9092"
            schema_registry_url: # Your Kafka Schema Registry url, e.g. http://schemaregistry:8081
sink: 
    type: datahub-rest 
    config: 
        server: "${baseUrl}/api/gms"
        token: # Paste a Personal Access Token here`;

const kafkaConfig: SourceConfig = {
    type: 'kafka',
    placeholderRecipe,
    displayName: 'Kafka',
    docsUrl: 'https://datahubproject.io/docs/metadata-ingestion/source_docs/kafka/',
    logoUrl: kafkaLogo,
};

export default kafkaConfig;
