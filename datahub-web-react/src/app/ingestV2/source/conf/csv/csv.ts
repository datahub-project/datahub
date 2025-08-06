import { SourceConfig } from '@app/ingestV2/source/conf/types';

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
