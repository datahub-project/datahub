/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SourceConfig } from '@app/ingestV2/source/conf/types';

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

const kafkaConfig: SourceConfig = {
    type: KAFKA,
    placeholderRecipe,
    displayName: 'Kafka',
    docsUrl: 'https://docs.datahub.com/docs/generated/ingestion/sources/kafka/',
    logoUrl: kafkaLogo,
};

export default kafkaConfig;
