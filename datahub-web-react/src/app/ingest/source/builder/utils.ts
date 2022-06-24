import Ajv from 'ajv';
import addFormats from 'ajv-formats';
import ingestionSchema from '../../../../assets/datahub_ingestion_schema.json';

type JsonSchema = typeof ingestionSchema;
interface FormattedSchema extends Omit<JsonSchema, '$schema' | 'id'> {
    $id: string;
    $schema?: string;
    id?: string;
}

function getFormattedSchema() {
    const formattedSchema: FormattedSchema = { ...ingestionSchema, $id: ingestionSchema.id }; // need id -> $id for ajv syntax
    delete formattedSchema.$schema; // ajv doesn't accept $schema
    delete formattedSchema.id;
    return formattedSchema;
}

export function getSchemaValidator() {
    const ajv = new Ajv();
    addFormats(ajv);
    ajv.addFormat('directory-path', '.*'); // not supported in ajv validation, match anything
    ajv.addFormat('time-delta', '.*'); // not supported in ajv validation, match anything

    const formattedSchema = getFormattedSchema();
    const validate = ajv.compile(formattedSchema);

    return validate;
}
