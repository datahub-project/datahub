/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { SourceConfig } from '@app/ingest/source/conf/types';

import csvLogo from '@images/csv-logo.png';

const placeholderRecipe = `\
source:
    type: csv-enricher
    config:
        filename: # URL of your csv file to ingest, e.g. https://docs.google.com/spreadsheets/d/DOCID/export?format=csv
        array_delimiter: |
        delimiter: ,
        write_semantics: PATCH
`;

const csvConfig: SourceConfig = {
    type: 'csv-enricher',
    placeholderRecipe,
    displayName: 'CSV',
    docsUrl: 'https://docs.datahub.com/docs/generated/ingestion/sources/csv-enricher',
    logoUrl: csvLogo,
};

export default csvConfig;
