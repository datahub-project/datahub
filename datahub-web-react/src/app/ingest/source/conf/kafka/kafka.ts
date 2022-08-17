import { SourceConfig } from '../types';
import kafkaLogo from '../../../../../images/kafkalogo.png';

const placeholderRecipe = `\
source:
    type: kafka
    config:
        connection:
            consumer_config:
                security.protocol: "SASL_SSL"
                sasl.mechanism: "PLAIN"
        stateful_ingestion:
            enabled: true
`;

export const KAFKA = 'kafka';

const kafkaConfig: SourceConfig = {
    type: KAFKA,
    placeholderRecipe,
    displayName: 'Kafka',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/kafka/',
    logoUrl: kafkaLogo,
};

export default kafkaConfig;
