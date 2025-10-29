import { SourceConfig } from '@app/ingest/source/conf/types';

import kafkaLogo from '@images/kafkalogo.png';

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

