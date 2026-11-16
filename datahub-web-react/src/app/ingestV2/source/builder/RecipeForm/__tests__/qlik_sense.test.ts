import { describe, expect, it } from 'vitest';

import { FieldType, RecipeField, setFieldValueOnRecipe } from '@app/ingestV2/source/builder/RecipeForm/common';
import { RECIPE_FIELDS } from '@app/ingestV2/source/builder/RecipeForm/constants';
import {
    QLIK_API_KEY,
    QLIK_DATA_CONNECTION_MAP,
    QLIK_DEPLOYMENT,
    QLIK_DEPLOYMENT_CLOUD,
    QLIK_DEPLOYMENT_ON_PREM,
    QLIK_FILE_CONNECTION_MAP,
    QLIK_INCLUDE_SHEETS_AND_CHARTS,
    QLIK_INGEST_OWNER,
    QLIK_ON_PREM_CLIENT_CERT,
    QLIK_ON_PREM_CLIENT_KEY_PASSWORD,
    QLIK_ON_PREM_READ_DATA_MODEL,
    QLIK_ON_PREM_SERVER,
    QLIK_ON_PREM_SKIP_HOSTNAME_CHECK,
    QLIK_ON_PREM_SKIP_UNCHANGED_APPS,
    QLIK_SCRIPT_VARIABLES,
    QLIK_SPACE_ALLOW,
    QLIK_SPACE_DENY,
    QLIK_TENANT_HOSTNAME,
    QLIK_TIMEOUT_SECONDS,
    integerRangeRule,
    setIntegerValueOnRecipe,
    tenantHostnameRule,
    uniqueDataConnectionNamesRule,
} from '@app/ingestV2/source/builder/RecipeForm/qlik_sense';
import { QLIK_SENSE } from '@app/ingestV2/source/builder/constants';

const spec = RECIPE_FIELDS[QLIK_SENSE];
const allFields = [...spec.fields, ...spec.advancedFields, ...spec.filterFields];

// Mirrors RecipeForm.updateRecipe: apply each changed value, then clear fields hidden
// for the selected deployment.
function applyFormValues(recipe: any, values: Record<string, unknown>): any {
    let updated = recipe;
    Object.entries(values).forEach(([name, value]) => {
        const field = allFields.find((f) => f.name === name) as RecipeField;
        updated = field.setValueOnRecipeOverride
            ? field.setValueOnRecipeOverride(updated, value)
            : setFieldValueOnRecipe(updated, value, field.fieldPath);
    });
    allFields
        .filter((field) => field.dynamicHidden?.(values))
        .forEach((field) => {
            updated = setFieldValueOnRecipe(updated, null, field.fieldPath);
        });
    return updated;
}

const baseRecipe = () => ({ source: { type: 'qlik-sense', config: {} } });

describe('Qlik Sense recipe form', () => {
    it('registers the deployment selector first, space filters, and the API key as a secret', () => {
        expect(spec.fields[0]).toBe(QLIK_DEPLOYMENT);
        expect(spec.filterFields).toEqual([QLIK_SPACE_ALLOW, QLIK_SPACE_DENY]);
        expect(QLIK_SPACE_ALLOW.fieldPath).toBe('source.config.space_pattern.allow');
        expect(QLIK_API_KEY.type).toBe(FieldType.SECRET);
    });

    it('shows and requires only the fields of the selected deployment', () => {
        const cloud = { deployment: QLIK_DEPLOYMENT_CLOUD };
        const onPrem = { deployment: QLIK_DEPLOYMENT_ON_PREM };

        expect(QLIK_TENANT_HOSTNAME.dynamicHidden?.(cloud)).toBe(false);
        expect(QLIK_TENANT_HOSTNAME.dynamicRequired?.(cloud)).toBe(true);
        expect(QLIK_TENANT_HOSTNAME.dynamicHidden?.(onPrem)).toBe(true);
        expect(QLIK_ON_PREM_SERVER.dynamicHidden?.(cloud)).toBe(true);
        expect(QLIK_ON_PREM_CLIENT_CERT.dynamicRequired?.(onPrem)).toBe(true);
        expect(QLIK_ON_PREM_SKIP_UNCHANGED_APPS.dynamicHidden?.(cloud)).toBe(true);
        expect(QLIK_ON_PREM_READ_DATA_MODEL.dynamicHidden?.(cloud)).toBe(true);
        // Sheets and charts apply to both deployments and stay on unless turned off.
        expect(QLIK_INCLUDE_SHEETS_AND_CHARTS.dynamicHidden).toBeUndefined();
        expect(QLIK_INCLUDE_SHEETS_AND_CHARTS.getValueFromRecipeOverride?.({})).toBe(true);
        // Off unless the recipe turns it on, as in the connector.
        expect(QLIK_ON_PREM_READ_DATA_MODEL.getValueFromRecipeOverride?.({})).toBe(false);
        expect(
            QLIK_ON_PREM_READ_DATA_MODEL.getValueFromRecipeOverride?.({
                source: { config: { on_prem: { read_data_model: true } } },
            }),
        ).toBe(true);
        expect(QLIK_ON_PREM_CLIENT_KEY_PASSWORD.dynamicHidden?.(cloud)).toBe(true);
        expect(QLIK_ON_PREM_CLIENT_KEY_PASSWORD.dynamicHidden?.(onPrem)).toBe(false);
        expect(QLIK_ON_PREM_CLIENT_KEY_PASSWORD.type).toBe(FieldType.SECRET);
        expect(QLIK_TIMEOUT_SECONDS.dynamicHidden).toBeUndefined();
    });

    it('infers the deployment from whether the recipe has an on_prem block', () => {
        expect(QLIK_DEPLOYMENT.getValueFromRecipeOverride?.({ source: { config: {} } })).toBe(QLIK_DEPLOYMENT_CLOUD);
        expect(QLIK_DEPLOYMENT.getValueFromRecipeOverride?.({ source: { config: { on_prem: { server: 'h' } } } })).toBe(
            QLIK_DEPLOYMENT_ON_PREM,
        );
    });

    it('builds a Qlik Cloud recipe', () => {
        const recipe = applyFormValues(baseRecipe(), {
            deployment: QLIK_DEPLOYMENT_CLOUD,
            tenant_hostname: 'example.us.qlikcloud.com',
            api_key: 'api-key-value',
            'space_pattern.allow': ['Finance'],
            max_workers: '8',
            timeout_seconds: '600',
        });
        expect(recipe.source.config).toEqual({
            tenant_hostname: 'example.us.qlikcloud.com',
            api_key: 'api-key-value',
            space_pattern: { allow: ['Finance'] },
            max_workers: 8,
            timeout_seconds: 600,
        });
    });

    it('builds an on-prem recipe with the connection nested under on_prem', () => {
        const recipe = applyFormValues(baseRecipe(), {
            deployment: QLIK_DEPLOYMENT_ON_PREM,
            'on_prem.server': '10.0.0.10',
            'on_prem.client_cert': '/secrets/client.pem',
            'on_prem.client_key': '/secrets/client_key.pem',
            'on_prem.client_key_password': 'key-password',
            'on_prem.root_cert': '/secrets/root.pem',
            'on_prem.server_cert_hostname': 'qlik.example.com',
            'on_prem.qrs_port': '4243',
            'on_prem.ingest_unpublished_apps': true,
            'on_prem.skip_unchanged_apps': true,
            'stateful_ingestion.enabled': true,
        });
        expect(recipe.source.config).toEqual({
            on_prem: {
                server: '10.0.0.10',
                client_cert: '/secrets/client.pem',
                client_key: '/secrets/client_key.pem',
                client_key_password: 'key-password',
                root_cert: '/secrets/root.pem',
                server_cert_hostname: 'qlik.example.com',
                qrs_port: 4243,
                ingest_unpublished_apps: true,
                skip_unchanged_apps: true,
            },
            stateful_ingestion: { enabled: true },
        });
    });

    it('drops the other deployment settings when switching, since the connector rejects both', () => {
        const cloudRecipe = {
            source: { config: { tenant_hostname: 'example.us.qlikcloud.com', api_key: 'k', ingest_owner: true } },
        };
        const toOnPrem = QLIK_DEPLOYMENT.setValueOnRecipeOverride?.(cloudRecipe, QLIK_DEPLOYMENT_ON_PREM);
        expect(toOnPrem.source.config).toEqual({ ingest_owner: true });

        const onPremRecipe = { source: { config: { on_prem: { server: 'h' }, ingest_owner: false } } };
        const toCloud = QLIK_DEPLOYMENT.setValueOnRecipeOverride?.(onPremRecipe, QLIK_DEPLOYMENT_CLOUD);
        expect(toCloud.source.config).toEqual({ ingest_owner: false });
        expect(toCloud.source.config.deployment).toBeUndefined();
    });

    it('writes verify_server_hostname: false only when the name check is skipped', () => {
        const base = { source: { config: { on_prem: { server: 'h' } } } };
        const skipped = QLIK_ON_PREM_SKIP_HOSTNAME_CHECK.setValueOnRecipeOverride?.(base, true);
        expect(skipped.source.config.on_prem.verify_server_hostname).toBe(false);
        expect(QLIK_ON_PREM_SKIP_HOSTNAME_CHECK.getValueFromRecipeOverride?.(skipped)).toBe(true);

        const checked = QLIK_ON_PREM_SKIP_HOSTNAME_CHECK.setValueOnRecipeOverride?.(skipped, false);
        expect(checked.source.config.on_prem).toEqual({ server: 'h' });
        expect(QLIK_ON_PREM_SKIP_HOSTNAME_CHECK.getValueFromRecipeOverride?.(checked)).toBe(false);
    });

    it('shows ingest owners as on unless the recipe turns it off, matching the connector default', () => {
        expect(QLIK_INGEST_OWNER.getValueFromRecipeOverride?.({ source: { config: {} } })).toBe(true);
        expect(QLIK_INGEST_OWNER.getValueFromRecipeOverride?.({ source: { config: { ingest_owner: false } } })).toBe(
            false,
        );
    });

    it('writes whole numbers as integers and clears empty input', () => {
        const path = 'source.config.max_workers';
        expect(setIntegerValueOnRecipe(baseRecipe(), ' 6 ', path).source.config.max_workers).toBe(6);
        expect(setIntegerValueOnRecipe(baseRecipe(), 'six', path).source.config.max_workers).toBe('six');
        const cleared = setIntegerValueOnRecipe(
            { source: { config: { max_workers: 6, ingest_owner: true } } },
            '',
            path,
        );
        expect(cleared.source.config).toEqual({ ingest_owner: true });
    });

    it('round-trips the data connection mapping and leaves empty details out', () => {
        const recipe = QLIK_DATA_CONNECTION_MAP.setValueOnRecipeOverride?.(baseRecipe(), [
            {
                data_connection_name: 'Sales DB',
                platform: 'mssql',
                platform_instance: '',
                env: '',
                database: 'sales_db',
                default_schema: 'dbo',
            },
            { data_connection_name: '', platform: 'snowflake' },
        ]);
        expect(recipe.source.config.data_connection_to_platform_instance).toEqual({
            'Sales DB': { platform: 'mssql', database: 'sales_db', default_schema: 'dbo' },
        });
        expect(QLIK_DATA_CONNECTION_MAP.getValueFromRecipeOverride?.(recipe)).toEqual([
            {
                data_connection_name: 'Sales DB',
                platform: 'mssql',
                platform_instance: '',
                env: '',
                database: 'sales_db',
                default_schema: 'dbo',
            },
        ]);
    });

    it('round-trips the file connection mapping, shown for Qlik Cloud only', () => {
        const recipe = QLIK_FILE_CONNECTION_MAP.setValueOnRecipeOverride?.(baseRecipe(), [
            {
                data_connection_name: 'Sales Space:Lake',
                storage_uri: 'abfss://lake@account.dfs.core.windows.net/sales',
                platform_instance: '',
                env: '',
            },
        ]);
        expect(recipe.source.config.file_connection_to_storage).toEqual({
            'Sales Space:Lake': { storage_uri: 'abfss://lake@account.dfs.core.windows.net/sales' },
        });
        expect(QLIK_FILE_CONNECTION_MAP.getValueFromRecipeOverride?.(recipe)).toEqual([
            {
                data_connection_name: 'Sales Space:Lake',
                storage_uri: 'abfss://lake@account.dfs.core.windows.net/sales',
                platform_instance: '',
                env: '',
            },
        ]);
        expect(QLIK_FILE_CONNECTION_MAP.dynamicHidden?.({ deployment: 'on_prem' })).toBe(true);

        // A setting the form has no field for survives a save.
        const withFlag = {
            source: {
                type: 'qlik-sense',
                config: {
                    file_connection_to_storage: {
                        'Sales Space:Lake': { storage_uri: 's3://old', convert_urns_to_lowercase: true },
                    },
                },
            },
        };
        const saved = QLIK_FILE_CONNECTION_MAP.setValueOnRecipeOverride?.(withFlag, [
            { data_connection_name: 'Sales Space:Lake', storage_uri: 's3://new' },
        ]);
        expect(saved.source.config.file_connection_to_storage).toEqual({
            'Sales Space:Lake': { storage_uri: 's3://new', convert_urns_to_lowercase: true },
        });
    });

    it('round-trips script variables as a name to value mapping, keeping empty values', () => {
        const recipe = QLIK_SCRIPT_VARIABLES.setValueOnRecipeOverride?.(baseRecipe(), [
            { variable_name: 'vSchema', value: 'sales' },
            { variable_name: 'vPrefix', value: '' },
            { variable_name: '', value: 'ignored' },
        ]);
        expect(recipe.source.config.script_variables).toEqual({ vSchema: 'sales', vPrefix: '' });
        expect(QLIK_SCRIPT_VARIABLES.getValueFromRecipeOverride?.(recipe)).toEqual([
            { variable_name: 'vSchema', value: 'sales' },
            { variable_name: 'vPrefix', value: '' },
        ]);
    });

    it('accepts a bare script variable name and rejects $() or surrounding spaces', () => {
        const nameRule = QLIK_SCRIPT_VARIABLES.keyField?.rules?.find((rule) => 'pattern' in rule) as {
            pattern: RegExp;
        };
        expect(nameRule.pattern.test('vSchema')).toBe(true);
        expect(nameRule.pattern.test('v')).toBe(true);
        expect(nameRule.pattern.test('$(vSchema)')).toBe(false);
        expect(nameRule.pattern.test(' vSchema')).toBe(false);
    });

    it('accepts a bare tenant hostname and rejects a scheme or path', () => {
        expect(tenantHostnameRule.pattern.test('example.us.qlikcloud.com')).toBe(true);
        expect(tenantHostnameRule.pattern.test('https://example.us.qlikcloud.com')).toBe(false);
        expect(tenantHostnameRule.pattern.test('example.us.qlikcloud.com/hub')).toBe(false);
    });

    it('limits ports to 1-65535 and max workers to at least 1, as the connector does', async () => {
        const port = integerRangeRule(1, 65535);
        await expect(port.validator(null, '4242')).resolves.toBeUndefined();
        await expect(port.validator(null, 4747)).resolves.toBeUndefined();
        await expect(port.validator(null, '')).resolves.toBeUndefined();
        await expect(port.validator(null, '0')).rejects.toThrow();
        await expect(port.validator(null, '65536')).rejects.toThrow();
        await expect(port.validator(null, '42.5')).rejects.toThrow();

        const workers = integerRangeRule(1);
        await expect(workers.validator(null, '64')).resolves.toBeUndefined();
        await expect(workers.validator(null, '0')).rejects.toThrow();
    });

    it('rejects mapping rows that repeat a data connection name', async () => {
        await expect(
            uniqueDataConnectionNamesRule.validator(null, [
                { data_connection_name: 'Sales DB', platform: 'mssql' },
                { data_connection_name: 'HR DB', platform: 'mssql' },
                { data_connection_name: '' },
            ]),
        ).resolves.toBeUndefined();
        await expect(
            uniqueDataConnectionNamesRule.validator(null, [
                { data_connection_name: 'Sales DB', platform: 'mssql' },
                { data_connection_name: 'Sales DB', platform: 'snowflake' },
            ]),
        ).rejects.toThrow('Sales DB');
    });
});
