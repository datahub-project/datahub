import get from 'lodash/get';
import omit from 'lodash/omit';

import {
    FieldType,
    FieldsValues,
    FilterRecipeField,
    FilterRule,
    RecipeField,
    setFieldValueOnRecipe,
    setListValuesOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/common';

export const QLIK_DEPLOYMENT_CLOUD = 'cloud';
export const QLIK_DEPLOYMENT_ON_PREM = 'on_prem';

const deploymentFieldName = 'deployment';
const deploymentFieldPath = 'source.config.deployment';
const tenantHostnameFieldPath = 'source.config.tenant_hostname';
const apiKeyFieldPath = 'source.config.api_key';
const onPremFieldPath = 'source.config.on_prem';

const isOnPrem = (values: FieldsValues) => values?.[deploymentFieldName] === QLIK_DEPLOYMENT_ON_PREM;
const isCloud = (values: FieldsValues) => !isOnPrem(values);

// The connector rejects a recipe that sets both on_prem and tenant_hostname/api_key,
// so switching deployment drops the other deployment's settings. `deployment` itself
// is form-only: the connector infers it from whether on_prem is set.
export function setQlikDeploymentOnRecipe(recipe: any, value: string | undefined): any {
    const updatedRecipe = omit({ ...recipe }, [deploymentFieldPath]);
    if (value === QLIK_DEPLOYMENT_ON_PREM) {
        return omit(updatedRecipe, [tenantHostnameFieldPath, apiKeyFieldPath]);
    }
    return omit(updatedRecipe, [onPremFieldPath]);
}

export function getQlikDeploymentFromRecipe(recipe: any): string {
    return get(recipe, onPremFieldPath) ? QLIK_DEPLOYMENT_ON_PREM : QLIK_DEPLOYMENT_CLOUD;
}

// TEXT inputs produce strings; write whole numbers as integers so the YAML matches the
// connector's int fields. Anything else is written as typed so the connector reports it.
export function setIntegerValueOnRecipe(recipe: any, value: string | number | undefined, fieldPath: string): any {
    const trimmed = typeof value === 'string' ? value.trim() : value;
    if (trimmed === undefined || trimmed === null || trimmed === '') {
        return setFieldValueOnRecipe(recipe, null, fieldPath);
    }
    const parsed = Number(trimmed);
    return setFieldValueOnRecipe(recipe, Number.isInteger(parsed) ? parsed : trimmed, fieldPath);
}

// Matches the connector's bounds: ports are 1-65535 and max_workers is at least 1.
export function integerRangeRule(min: number, max?: number) {
    const message =
        max === undefined ? `Enter a whole number of at least ${min}` : `Enter a whole number from ${min} to ${max}`;
    return {
        validator(_: unknown, value: string | number | undefined | null) {
            const text = value === undefined || value === null ? '' : String(value).trim();
            if (text === '') return Promise.resolve();
            const parsed = Number(text);
            if (!/^\d+$/.test(text) || parsed < min || (max !== undefined && parsed > max)) {
                return Promise.reject(new Error(message));
            }
            return Promise.resolve();
        },
    };
}

const portRule = integerRangeRule(1, 65535);

// The connector builds https://<tenant_hostname>/api/v1, so a scheme or path breaks every call.
export const tenantHostnameRule = {
    pattern: /^[^\s/]+$/,
    message: 'Enter the hostname only, without https:// or a path',
};

export const QLIK_DEPLOYMENT: RecipeField = {
    name: deploymentFieldName,
    label: 'Deployment Type',
    helper: 'Qlik Cloud or Qlik Sense Enterprise on Windows',
    tooltip:
        'Qlik Cloud connects with a tenant hostname and API key. Qlik Sense Enterprise on Windows connects to the central node with client certificates exported from the QMC.',
    type: FieldType.SELECT,
    fieldPath: deploymentFieldPath,
    options: [
        { label: 'Qlik Cloud', value: QLIK_DEPLOYMENT_CLOUD },
        { label: 'Qlik Sense Enterprise on Windows', value: QLIK_DEPLOYMENT_ON_PREM },
    ],
    required: true,
    rules: null,
    setValueOnRecipeOverride: setQlikDeploymentOnRecipe,
    getValueFromRecipeOverride: getQlikDeploymentFromRecipe,
};

export const QLIK_TENANT_HOSTNAME: RecipeField = {
    name: 'tenant_hostname',
    label: 'Tenant Hostname',
    helper: 'Hostname only, without https://',
    tooltip:
        'Hostname of the Qlik Cloud tenant, shown on the About page of the hub. Enter the hostname only, without https:// or a path.',
    type: FieldType.TEXT,
    fieldPath: tenantHostnameFieldPath,
    placeholder: 'example.us.qlikcloud.com',
    // Only rendered, and so only validated, when Qlik Cloud is selected.
    rules: [{ required: true, message: 'Tenant Hostname is required' }, tenantHostnameRule],
    dynamicHidden: isOnPrem,
    dynamicRequired: isCloud,
};

export const QLIK_API_KEY: RecipeField = {
    name: 'api_key',
    label: 'API Key',
    helper: 'API key generated in the Qlik Cloud hub',
    tooltip:
        'Qlik Cloud API key. Ingestion only sees the spaces, apps and data connections that the user who generated the key can access.',
    type: FieldType.SECRET,
    fieldPath: apiKeyFieldPath,
    placeholder: 'api_key',
    rules: null,
    dynamicHidden: isOnPrem,
    dynamicRequired: isCloud,
};

export const QLIK_ON_PREM_SERVER: RecipeField = {
    name: 'on_prem.server',
    label: 'Server',
    helper: 'Central node hostname',
    tooltip:
        'Hostname or IP address of the Qlik Sense central node. The Repository Service and Engine ports are reached on this host.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.on_prem.server',
    placeholder: 'qlik.example.com',
    rules: null,
    dynamicHidden: isCloud,
    dynamicRequired: isOnPrem,
};

const certificatePathTooltip = (file: string) =>
    `Path to the PEM ${file} exported from the QMC, not the file contents. The file must exist on the machine that runs ingestion, so UI ingestion must run on an executor where the certificate files are mounted (on DataHub Cloud, a remote executor).`;

export const QLIK_ON_PREM_CLIENT_CERT: RecipeField = {
    name: 'on_prem.client_cert',
    label: 'Client Certificate Path',
    helper: 'Path to client.pem on the executor where it is mounted',
    tooltip: certificatePathTooltip('client certificate'),
    type: FieldType.TEXT,
    fieldPath: 'source.config.on_prem.client_cert',
    placeholder: '/secrets/client.pem',
    rules: null,
    dynamicHidden: isCloud,
    dynamicRequired: isOnPrem,
};

export const QLIK_ON_PREM_CLIENT_KEY: RecipeField = {
    name: 'on_prem.client_key',
    label: 'Client Key Path',
    helper: 'Path to client_key.pem on the executor where it is mounted',
    tooltip: certificatePathTooltip('client key'),
    type: FieldType.TEXT,
    fieldPath: 'source.config.on_prem.client_key',
    placeholder: '/secrets/client_key.pem',
    rules: null,
    dynamicHidden: isCloud,
    dynamicRequired: isOnPrem,
};

export const QLIK_ON_PREM_CLIENT_KEY_PASSWORD: RecipeField = {
    name: 'on_prem.client_key_password',
    label: 'Client Key Password',
    helper: 'Only if the client key was exported with a password',
    tooltip:
        'Password of the client key, when it was exported from the QMC with one. Leave empty for an unencrypted key.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.on_prem.client_key_password',
    placeholder: 'client_key_password',
    rules: null,
    dynamicHidden: isCloud,
};

export const QLIK_ON_PREM_ROOT_CERT: RecipeField = {
    name: 'on_prem.root_cert',
    label: 'Root Certificate Path',
    helper: 'Path to root.pem on the executor where it is mounted',
    tooltip: `${certificatePathTooltip('root certificate')} The server certificate is verified against it.`,
    type: FieldType.TEXT,
    fieldPath: 'source.config.on_prem.root_cert',
    placeholder: '/secrets/root.pem',
    rules: null,
    dynamicHidden: isCloud,
    dynamicRequired: isOnPrem,
};

export const QLIK_ON_PREM_SERVER_CERT_HOSTNAME: RecipeField = {
    name: 'on_prem.server_cert_hostname',
    label: 'Server Certificate Hostname',
    helper: 'Name on the server certificate, if it differs from Server',
    tooltip:
        'Hostname the Qlik server certificate was issued to. Set it when Server is an IP address or alias that does not match the certificate. It is also used for links to Qlik.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.on_prem.server_cert_hostname',
    placeholder: 'qlik-central.example.com',
    rules: null,
    dynamicHidden: isCloud,
};

// Checked writes verify_server_hostname: false; unchecked drops the key so the
// connector keeps its default of checking the certificate name.
const verifyServerHostnameFieldPath = 'source.config.on_prem.verify_server_hostname';
export const QLIK_ON_PREM_SKIP_HOSTNAME_CHECK: RecipeField = {
    name: 'on_prem.skip_server_hostname_check',
    label: 'Skip Certificate Name Check',
    helper: 'Last resort when no hostname matches the certificate',
    tooltip:
        'Accept any server certificate signed by the root certificate, whatever name it was issued to. The certificate chain is still verified. Try Server Certificate Hostname first.',
    type: FieldType.BOOLEAN,
    fieldPath: verifyServerHostnameFieldPath,
    rules: null,
    dynamicHidden: isCloud,
    getValueFromRecipeOverride: (recipe: any) => get(recipe, verifyServerHostnameFieldPath) === false,
    setValueOnRecipeOverride: (recipe: any, value: boolean) => {
        if (value) return setFieldValueOnRecipe(recipe, false, verifyServerHostnameFieldPath);
        return omit({ ...recipe }, [verifyServerHostnameFieldPath]);
    },
};

const qrsPortFieldPath = 'source.config.on_prem.qrs_port';
export const QLIK_ON_PREM_QRS_PORT: RecipeField = {
    name: 'on_prem.qrs_port',
    label: 'Repository Service Port',
    helper: 'Defaults to 4242',
    tooltip: 'Qlik Repository Service (QRS) port, used to list streams, apps and data connections. Defaults to 4242.',
    type: FieldType.TEXT,
    fieldPath: qrsPortFieldPath,
    placeholder: '4242',
    rules: [portRule],
    dynamicHidden: isCloud,
    setValueOnRecipeOverride: (recipe: any, value: string) => setIntegerValueOnRecipe(recipe, value, qrsPortFieldPath),
};

const enginePortFieldPath = 'source.config.on_prem.engine_port';
export const QLIK_ON_PREM_ENGINE_PORT: RecipeField = {
    name: 'on_prem.engine_port',
    label: 'Engine Port',
    helper: 'Defaults to 4747',
    tooltip: 'Qlik Engine port, used to read app load scripts, sheets and charts. Defaults to 4747.',
    type: FieldType.TEXT,
    fieldPath: enginePortFieldPath,
    placeholder: '4747',
    rules: [portRule],
    dynamicHidden: isCloud,
    setValueOnRecipeOverride: (recipe: any, value: string) =>
        setIntegerValueOnRecipe(recipe, value, enginePortFieldPath),
};

const spaceAllowFieldPath = 'source.config.space_pattern.allow';
export const QLIK_SPACE_ALLOW: FilterRecipeField = {
    name: 'space_pattern.allow',
    label: 'Allow Patterns',
    helper: 'Include specific spaces or streams',
    tooltip:
        'Only include Qlik Cloud spaces, or streams on Qlik Sense Enterprise, whose name matches one of these regular expressions. Use personal_space for Qlik Cloud personal spaces, and Unpublished for on-prem apps not published to a stream.',
    placeholder: 'Finance',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: spaceAllowFieldPath,
    rules: null,
    section: 'Spaces and Streams',
    filteringResource: 'Space',
    rule: FilterRule.INCLUDE,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, spaceAllowFieldPath),
};

const spaceDenyFieldPath = 'source.config.space_pattern.deny';
export const QLIK_SPACE_DENY: FilterRecipeField = {
    name: 'space_pattern.deny',
    label: 'Deny Patterns',
    helper: 'Exclude specific spaces or streams',
    tooltip:
        'Exclude Qlik Cloud spaces, or streams on Qlik Sense Enterprise, whose name matches one of these regular expressions. Deny patterns take precedence over allow patterns.',
    placeholder: 'personal_space',
    type: FieldType.LIST,
    buttonLabel: 'Add pattern',
    fieldPath: spaceDenyFieldPath,
    rules: null,
    section: 'Spaces and Streams',
    filteringResource: 'Space',
    rule: FilterRule.EXCLUDE,
    setValueOnRecipeOverride: (recipe: any, values: string[]) =>
        setListValuesOnRecipe(recipe, values, spaceDenyFieldPath),
};

// The connector ingests owners unless the recipe sets ingest_owner: false.
const includeSheetsAndChartsFieldPath = 'source.config.include_sheets_and_charts';
export const QLIK_INCLUDE_SHEETS_AND_CHARTS: RecipeField = {
    name: 'include_sheets_and_charts',
    label: 'Include Sheets and Charts',
    helper: "Ingest each app's sheets and charts",
    tooltip:
        "Ingest each app's sheets as dashboards and their charts. Turn off to keep only apps and their tables: each app dashboard then links only to its tables, and sheets and charts ingested earlier are removed when stale metadata removal is on. As they are usually most of the ingested entities, set stateful_ingestion.fail_safe_threshold to 100 in the recipe for that first run, or stale removal is skipped.",
    type: FieldType.BOOLEAN,
    fieldPath: includeSheetsAndChartsFieldPath,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => get(recipe, includeSheetsAndChartsFieldPath) !== false,
};

const ingestOwnerFieldPath = 'source.config.ingest_owner';
export const QLIK_INGEST_OWNER: RecipeField = {
    name: 'ingest_owner',
    label: 'Ingest Owners',
    helper: 'Set Qlik owners as DataHub owners',
    tooltip:
        'Add the Qlik owner of each space or stream, app, sheet and dataset as its DataHub owner. This overwrites owners added in DataHub on those entities.',
    type: FieldType.BOOLEAN,
    fieldPath: ingestOwnerFieldPath,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => get(recipe, ingestOwnerFieldPath) !== false,
};

const maxWorkersFieldPath = 'source.config.max_workers';
// Left unset by default: the connector reads 1 app at a time when it loads app data, as on
// Qlik Cloud and on-prem by default, and 4 on-prem when apps are opened without their data.
export const QLIK_MAX_WORKERS: RecipeField = {
    name: 'max_workers',
    label: 'Max Workers',
    helper: 'Apps read at the same time. Defaults to 1, or 4 on-prem without app data',
    tooltip:
        'Number of apps and datasets read at the same time, across all spaces and streams. Each app read holds one Engine session open. Defaults to 1, as every open app loads its data into Engine memory, and to 4 on Qlik Sense Enterprise when Read Data Model is turned off. Lower it if Qlik Cloud rate-limits the run.',
    type: FieldType.TEXT,
    fieldPath: maxWorkersFieldPath,
    placeholder: '1',
    rules: [integerRangeRule(1)],
    setValueOnRecipeOverride: (recipe: any, value: string) =>
        setIntegerValueOnRecipe(recipe, value, maxWorkersFieldPath),
};

const timeoutSecondsFieldPath = 'source.config.timeout_seconds';
export const QLIK_TIMEOUT_SECONDS: RecipeField = {
    name: 'timeout_seconds',
    label: 'Timeout (Seconds)',
    helper: 'Wait for a Qlik reply before failing. Defaults to 300',
    tooltip:
        'Seconds to wait for the Qlik Engine, and on-prem for the Repository Service, to connect or reply before the request fails. An app whose read times out is ingested as partly read. Defaults to 300.',
    type: FieldType.TEXT,
    fieldPath: timeoutSecondsFieldPath,
    placeholder: '300',
    rules: [integerRangeRule(1)],
    setValueOnRecipeOverride: (recipe: any, value: string) =>
        setIntegerValueOnRecipe(recipe, value, timeoutSecondsFieldPath),
};

export const QLIK_ON_PREM_INGEST_UNPUBLISHED_APPS: RecipeField = {
    name: 'on_prem.ingest_unpublished_apps',
    label: 'Ingest Unpublished Apps',
    helper: 'Include apps not published to a stream',
    tooltip:
        'Also ingest apps that are not published to a stream. They are grouped under a container named Unpublished. Qlik Sense Enterprise on Windows only.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.on_prem.ingest_unpublished_apps',
    rules: null,
    dynamicHidden: isCloud,
};

const readDataModelFieldPath = 'source.config.on_prem.read_data_model';
export const QLIK_ON_PREM_READ_DATA_MODEL: RecipeField = {
    name: 'on_prem.read_data_model',
    label: 'Read Data Model',
    helper: 'Open apps with their data to read table fields and types',
    tooltip:
        'Open each app with its data to read its data model, which gives every table its fields and types, including tables loaded with LOAD * and from QVDs. Off by default, as it is much slower: the Engine keeps each app in memory until the app cache time set in the QMC has passed, and apps whose section access denies the service user their data cannot be read. Without it, table fields come only from the load script. Qlik Sense Enterprise on Windows only.',
    type: FieldType.BOOLEAN,
    fieldPath: readDataModelFieldPath,
    rules: null,
    getValueFromRecipeOverride: (recipe: any) => get(recipe, readDataModelFieldPath) === true,
    dynamicHidden: isCloud,
};

export const QLIK_ON_PREM_SKIP_UNCHANGED_APPS: RecipeField = {
    name: 'on_prem.skip_unchanged_apps',
    label: 'Skip Unchanged Apps',
    helper: 'Needs stateful ingestion. Only opens apps that changed since the last run',
    tooltip:
        'Do not read an app from the Engine again when it has not been saved, reloaded, published or moved, and the recipe, data connections and connector version are unchanged since the last run. Its metadata is kept. Needs stateful ingestion: if Enable Stateful Ingestion is turned off, every app is read and the run reports a warning. Qlik Sense Enterprise on Windows only.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.on_prem.skip_unchanged_apps',
    rules: null,
    dynamicHidden: isCloud,
};

const DATA_CONNECTION_NAME: RecipeField = {
    name: 'data_connection_name',
    label: 'Data Connection Name',
    helper: 'Name of the Qlik data connection',
    tooltip:
        'Name of the Qlik data connection as used in load scripts. On Qlik Sense Enterprise, personal connections include the owner suffix, for example Sales DB (domain_user).',
    type: FieldType.TEXT,
    fieldPath: 'data_connection_name',
    placeholder: 'Sales DB',
    rules: [{ required: true, message: 'Data Connection Name is required' }],
};

const DATA_CONNECTION_PLATFORM: RecipeField = {
    name: 'platform',
    label: 'Platform',
    helper: 'DataHub platform, e.g. mssql',
    tooltip: 'DataHub platform of the tables read through this connection. Inferred from the connection when empty.',
    type: FieldType.TEXT,
    fieldPath: 'platform',
    placeholder: 'mssql',
    rules: null,
};

const DATA_CONNECTION_PLATFORM_INSTANCE: RecipeField = {
    name: 'platform_instance',
    label: 'Platform Instance',
    helper: 'Platform instance of the upstream source',
    tooltip: 'Platform instance used when ingesting the upstream source, so lineage joins its datasets.',
    type: FieldType.TEXT,
    fieldPath: 'platform_instance',
    placeholder: 'prod_mssql',
    rules: null,
};

const DATA_CONNECTION_ENV: RecipeField = {
    name: 'env',
    label: 'Environment',
    helper: 'Env of the upstream source, e.g. PROD',
    tooltip: 'Environment used when ingesting the upstream source. Defaults to PROD.',
    type: FieldType.TEXT,
    fieldPath: 'env',
    placeholder: 'PROD',
    rules: null,
};

const DATA_CONNECTION_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database',
    helper: 'Database for unqualified tables',
    tooltip:
        'Database (catalog for Databricks) used for tables the load script does not qualify. Inferred from the connection string when empty.',
    type: FieldType.TEXT,
    fieldPath: 'database',
    placeholder: 'sales_db',
    rules: null,
};

const DATA_CONNECTION_DEFAULT_SCHEMA: RecipeField = {
    name: 'default_schema',
    label: 'Default Schema',
    helper: 'Schema for unqualified tables',
    tooltip: 'Schema used for tables the load script does not qualify.',
    type: FieldType.TEXT,
    fieldPath: 'default_schema',
    placeholder: 'dbo',
    rules: null,
};

const dataConnectionDetailFields = [
    DATA_CONNECTION_PLATFORM,
    DATA_CONNECTION_PLATFORM_INSTANCE,
    DATA_CONNECTION_ENV,
    DATA_CONNECTION_DATABASE,
    DATA_CONNECTION_DEFAULT_SCHEMA,
];

type MappingRow = Record<string, string | undefined>;

// Rows become keys of one mapping, so a repeated name would silently replace the earlier row.
const uniqueKeysRule = (keyField: RecipeField, label: string) => ({
    validator(_: unknown, rows: MappingRow[] | undefined) {
        const names = (rows ?? []).map((row) => row?.[keyField.name]).filter((name) => !!name);
        const duplicate = names.find((name, index) => names.indexOf(name) !== index);
        if (duplicate) {
            return Promise.reject(new Error(`${label} "${duplicate}" is used more than once`));
        }
        return Promise.resolve();
    },
});

export const uniqueDataConnectionNamesRule = uniqueKeysRule(DATA_CONNECTION_NAME, 'Data Connection Name');

// A recipe mapping of connection names to details, edited as one row per connection.
const connectionMappingOverrides = (fieldPath: string, keyField: RecipeField, detailFields: RecipeField[]) => ({
    getValueFromRecipeOverride: (recipe: any) => {
        const value = get(recipe, fieldPath);
        if (!value) return undefined;
        return Object.keys(value).map((connectionName) => ({
            [keyField.name]: connectionName,
            ...Object.fromEntries(detailFields.map((f) => [f.name, value[connectionName]?.[f.name] ?? ''])),
        }));
    },
    setValueOnRecipeOverride: (recipe: any, rows: MappingRow[] | undefined) => {
        const named = (rows ?? []).filter((row) => !!row?.[keyField.name]);
        if (!named.length) return setFieldValueOnRecipe(recipe, null, fieldPath);

        const current = get(recipe, fieldPath) ?? {};
        const formKeys = new Set(detailFields.map((f) => f.name));
        const mapping: Record<string, Record<string, unknown>> = {};
        named.forEach((row) => {
            const name = row[keyField.name] as string;
            // Settings the form has no field for, set in the YAML, are kept.
            const kept = Object.fromEntries(Object.entries(current[name] ?? {}).filter(([key]) => !formKeys.has(key)));
            // Empty inputs are left out so the connector infers or defaults them.
            mapping[name] = {
                ...kept,
                ...Object.fromEntries(detailFields.filter((f) => !!row[f.name]).map((f) => [f.name, row[f.name]])),
            };
        });
        return setFieldValueOnRecipe(recipe, mapping, fieldPath);
    },
});

export const QLIK_DATA_CONNECTION_MAP: RecipeField = {
    name: 'data_connection_to_platform_instance',
    label: 'Data Connection Mapping',
    helper: 'Map Qlik data connections to upstream platforms',
    tooltip:
        'Upstream platform details for each Qlik data connection, used to build lineage from load script SELECT statements. Only needed when they cannot be inferred from the connection, or when the upstream source uses a platform instance or a non-PROD env.',
    type: FieldType.DICT,
    buttonLabel: 'Add mapping',
    fieldPath: 'source.config.data_connection_to_platform_instance',
    keyField: DATA_CONNECTION_NAME,
    fields: dataConnectionDetailFields,
    rules: [uniqueDataConnectionNamesRule],
    ...connectionMappingOverrides(
        'source.config.data_connection_to_platform_instance',
        DATA_CONNECTION_NAME,
        dataConnectionDetailFields,
    ),
};

const FILE_CONNECTION_NAME: RecipeField = {
    ...DATA_CONNECTION_NAME,
    tooltip:
        'Name of the Azure Storage, Amazon S3 or Google Cloud Storage data connection as load scripts write it, for example Sales Space:Lake.',
    placeholder: 'Sales Space:Lake',
};

const FILE_CONNECTION_STORAGE_URI: RecipeField = {
    name: 'storage_uri',
    label: 'Storage URI',
    helper: 'abfss://, s3:// or gs:// URI of the files',
    tooltip:
        'Where the connection reads files from, for example abfss://container@account.dfs.core.windows.net/folder. Read from the data connection when empty, which needs the API key user to see the connection.',
    type: FieldType.TEXT,
    fieldPath: 'storage_uri',
    placeholder: 'abfss://container@account.dfs.core.windows.net/folder',
    rules: null,
};

const fileConnectionDetailFields = [
    FILE_CONNECTION_STORAGE_URI,
    {
        ...DATA_CONNECTION_PLATFORM_INSTANCE,
        tooltip: 'Platform instance used when ingesting the storage, so lineage joins its datasets.',
        placeholder: 'datalake',
    },
    DATA_CONNECTION_ENV,
];

export const QLIK_FILE_CONNECTION_MAP: RecipeField = {
    name: 'file_connection_to_storage',
    label: 'File Connection Mapping',
    helper: 'Map file data connections to their storage',
    tooltip:
        'Where the files of Azure Storage, Amazon S3 and Google Cloud Storage data connections are in DataHub, for lineage from the files apps load. Qlik Cloud only.',
    type: FieldType.DICT,
    buttonLabel: 'Add mapping',
    fieldPath: 'source.config.file_connection_to_storage',
    keyField: FILE_CONNECTION_NAME,
    fields: fileConnectionDetailFields,
    rules: [uniqueKeysRule(FILE_CONNECTION_NAME, 'Data Connection Name')],
    dynamicHidden: isOnPrem,
    ...connectionMappingOverrides(
        'source.config.file_connection_to_storage',
        FILE_CONNECTION_NAME,
        fileConnectionDetailFields,
    ),
};

const SCRIPT_VARIABLE_NAME: RecipeField = {
    name: 'variable_name',
    label: 'Variable Name',
    helper: 'Load script variable, without $()',
    tooltip: 'Name of a load script variable the connector cannot read, as the script writes it inside $().',
    type: FieldType.TEXT,
    fieldPath: 'variable_name',
    placeholder: 'vSchema',
    rules: [
        { required: true, message: 'Variable Name is required' },
        {
            pattern: /^[^$()=\s](?:[^$()=]*[^$()=\s])?$/,
            message: 'Enter the name alone, without $() or spaces around it',
        },
    ],
};

const SCRIPT_VARIABLE_VALUE: RecipeField = {
    name: 'value',
    label: 'Value',
    helper: 'The value to use',
    tooltip: 'The value the variable has when the app reloads.',
    type: FieldType.TEXT,
    fieldPath: 'value',
    placeholder: 'sales',
    rules: null,
};

const scriptVariablesFieldPath = 'source.config.script_variables';
export const QLIK_SCRIPT_VARIABLES: RecipeField = {
    name: 'script_variables',
    label: 'Script Variables',
    helper: 'Values for variables the connector cannot read',
    tooltip:
        "Values for load script variables the connector can't read, such as ones computed with LET, set in an included script, or deleted after the reload. Used in QVD and file paths, connection names, SQL and chart expressions. An app's own variables, as its last reload left them, take precedence.",
    type: FieldType.DICT,
    buttonLabel: 'Add variable',
    fieldPath: scriptVariablesFieldPath,
    keyField: SCRIPT_VARIABLE_NAME,
    fields: [SCRIPT_VARIABLE_VALUE],
    rules: [uniqueKeysRule(SCRIPT_VARIABLE_NAME, 'Variable Name')],
    getValueFromRecipeOverride: (recipe: any) => {
        const value: Record<string, string> | undefined = get(recipe, scriptVariablesFieldPath);
        if (!value) return undefined;
        return Object.entries(value).map(([name, variableValue]) => ({
            [SCRIPT_VARIABLE_NAME.name]: name,
            [SCRIPT_VARIABLE_VALUE.name]: variableValue,
        }));
    },
    setValueOnRecipeOverride: (recipe: any, rows: MappingRow[] | undefined) => {
        const named = (rows ?? []).filter((row) => !!row?.[SCRIPT_VARIABLE_NAME.name]);
        if (!named.length) return setFieldValueOnRecipe(recipe, null, scriptVariablesFieldPath);
        // An empty value is kept: a variable can be set to nothing.
        const variables = Object.fromEntries(
            named.map((row) => [row[SCRIPT_VARIABLE_NAME.name] as string, row[SCRIPT_VARIABLE_VALUE.name] ?? '']),
        );
        return setFieldValueOnRecipe(recipe, variables, scriptVariablesFieldPath);
    },
};
