import { SourceConfig } from '../types';
import csvLogo from '../../../../../images/csv-logo.png';

const placeholderRecipe = `\
source:
    type: csv-enricher
    config:
    filename: # relative path to your csv file to ingest, e.g. ./path/to/your/file.csv
    array_delimiter: |
    delimiter: ,
    write_semantics: PATCH
`;

const csvConfig: SourceConfig = {
    type: 'csv',
    placeholderRecipe,
    displayName: 'CSV',
    docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/csv',
    logoUrl: csvLogo,
};

export default csvConfig;
