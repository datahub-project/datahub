import get from 'lodash/get';
import omit from 'lodash/omit';

export const AUTH_TYPE_PAT = 'PAT';
export const AUTH_TYPE_OAUTH_M2M = 'OAUTH_M2M';
export const AUTH_TYPE_AZURE_AD = 'AZURE_AD';

const AUTH_TYPE_PATH = 'source.config.authentication_type';
const TOKEN_PATH = 'source.config.token';
const CLIENT_ID_PATH = 'source.config.client_id';
const CLIENT_SECRET_PATH = 'source.config.client_secret';
const AZURE_AUTH_PATH = 'source.config.azure_auth';

const PAT_CREDENTIAL_PATHS = [TOKEN_PATH];
const OAUTH_CREDENTIAL_PATHS = [CLIENT_ID_PATH, CLIENT_SECRET_PATH];
const AZURE_CREDENTIAL_PATHS = [AZURE_AUTH_PATH];

export type DatabricksFormValues = {
    authentication_type?: string;
    token?: string;
    client_id?: string;
    client_secret?: string;
    azure_auth?: {
        tenant_id?: string;
        client_id?: string;
        client_secret?: string;
    };
    // Flat dot-notation keys used by Ant Design forms
    'azure_auth.tenant_id'?: string;
    'azure_auth.client_id'?: string;
    'azure_auth.client_secret'?: string;
};

/**
 * Determines if a Databricks field should be visible based on authentication type.
 * Handles backward compatibility for existing configs that don't have authentication_type set.
 */
export function shouldShowDatabricksField(fieldName: string, formValues: DatabricksFormValues): boolean {
    const authenticationType = formValues?.authentication_type;

    // If authentication_type is explicitly set, use it to determine visibility
    if (authenticationType) {
        if (fieldName === 'token') {
            return authenticationType === AUTH_TYPE_PAT;
        }
        if (fieldName === 'client_id' || fieldName === 'client_secret') {
            return authenticationType === AUTH_TYPE_OAUTH_M2M;
        }
        if (
            fieldName === 'azure_auth.tenant_id' ||
            fieldName === 'azure_auth.client_id' ||
            fieldName === 'azure_auth.client_secret'
        ) {
            return authenticationType === AUTH_TYPE_AZURE_AD;
        }
        return true;
    }

    // Backward compatibility: detect auth type from existing credentials
    const hasToken = !!formValues?.token;
    const hasOAuthCreds = !!(formValues?.client_id || formValues?.client_secret);
    const azureAuth = formValues?.azure_auth;
    const hasAzureCreds = !!(
        azureAuth?.tenant_id ||
        azureAuth?.client_id ||
        azureAuth?.client_secret ||
        formValues?.['azure_auth.tenant_id'] ||
        formValues?.['azure_auth.client_id'] ||
        formValues?.['azure_auth.client_secret']
    );

    if (fieldName === 'token') {
        return hasToken || (!hasOAuthCreds && !hasAzureCreds);
    }
    if (fieldName === 'client_id' || fieldName === 'client_secret') {
        return hasOAuthCreds;
    }
    if (
        fieldName === 'azure_auth.tenant_id' ||
        fieldName === 'azure_auth.client_id' ||
        fieldName === 'azure_auth.client_secret'
    ) {
        return hasAzureCreds;
    }
    return true;
}

/**
 * Strips credentials belonging to the auth methods we are NOT using when the user
 * toggles the auth selector. Without this cleanup, the YAML would carry credentials
 * from more than one auth method and `UnityCatalogConnectionConfig.at_most_one_auth_method_provided`
 * in unity/connection.py would reject the recipe at ingestion time.
 *
 * `authentication_type` is intentionally NOT written to the recipe: it isn't a real
 * field on `UnityCatalogConnectionConfig`, and the connector inherits ConfigModel
 * which uses `extra="forbid"`, so writing it would itself fail validation. The form
 * value lives only in form state; on reload, `getDatabricksAuthTypeFromRecipe` infers
 * the selection from which credentials are populated.
 */
export function setDatabricksAuthTypeOnRecipe(recipe: any, value: string | undefined): any {
    let stalePaths: string[];
    if (value === AUTH_TYPE_PAT) {
        stalePaths = [...OAUTH_CREDENTIAL_PATHS, ...AZURE_CREDENTIAL_PATHS];
    } else if (value === AUTH_TYPE_OAUTH_M2M) {
        stalePaths = [...PAT_CREDENTIAL_PATHS, ...AZURE_CREDENTIAL_PATHS];
    } else if (value === AUTH_TYPE_AZURE_AD) {
        stalePaths = [...PAT_CREDENTIAL_PATHS, ...OAUTH_CREDENTIAL_PATHS];
    } else {
        // Unknown or cleared auth type — strip all credentials so we never leave the
        // recipe in an ambiguous state.
        stalePaths = [...PAT_CREDENTIAL_PATHS, ...OAUTH_CREDENTIAL_PATHS, ...AZURE_CREDENTIAL_PATHS];
    }
    // Also strip any legacy `authentication_type` left over from an earlier version of
    // the form that did write it — the connector would reject it.
    return omit({ ...recipe }, [AUTH_TYPE_PATH, ...stalePaths]);
}

/**
 * Infers the authentication type from existing recipe credentials. The connector does
 * not store `authentication_type` itself; it determines the auth method from which
 * credentials are populated. We mirror that order here so the form displays the right
 * option when editing.
 */
export function getDatabricksAuthTypeFromRecipe(recipe: any): string {
    if (get(recipe, AZURE_AUTH_PATH)) return AUTH_TYPE_AZURE_AD;
    if (get(recipe, CLIENT_ID_PATH) || get(recipe, CLIENT_SECRET_PATH)) return AUTH_TYPE_OAUTH_M2M;
    return AUTH_TYPE_PAT;
}

/**
 * Creates a validator that requires a field value when a specific authentication type is selected.
 */
export function createDatabricksAuthValidator(requiredAuthType: string, fieldLabel: string, authTypeLabel: string) {
    return ({ getFieldValue }) => ({
        validator(_, value) {
            const authType = getFieldValue('authentication_type');
            if (authType === requiredAuthType && !value) {
                return Promise.reject(new Error(`${fieldLabel} is required for ${authTypeLabel} authentication`));
            }
            return Promise.resolve();
        },
    });
}
