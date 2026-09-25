import {
    AUTH_TYPE_AZURE_AD,
    AUTH_TYPE_OAUTH_M2M,
    AUTH_TYPE_PAT,
    createDatabricksAuthValidator,
    getDatabricksAuthTypeFromRecipe,
    setDatabricksAuthTypeOnRecipe,
    shouldShowDatabricksField,
} from '@app/ingest/source/builder/RecipeForm/databricksAuth';

describe('shouldShowDatabricksField', () => {
    describe('with explicit authentication_type', () => {
        it('shows token for PAT, hides for others', () => {
            expect(shouldShowDatabricksField('token', { authentication_type: AUTH_TYPE_PAT })).toBe(true);
            expect(shouldShowDatabricksField('token', { authentication_type: AUTH_TYPE_OAUTH_M2M })).toBe(false);
            expect(shouldShowDatabricksField('token', { authentication_type: AUTH_TYPE_AZURE_AD })).toBe(false);
        });

        it('shows client_id/client_secret for OAUTH_M2M, hides for others', () => {
            expect(shouldShowDatabricksField('client_id', { authentication_type: AUTH_TYPE_OAUTH_M2M })).toBe(true);
            expect(shouldShowDatabricksField('client_secret', { authentication_type: AUTH_TYPE_OAUTH_M2M })).toBe(true);
            expect(shouldShowDatabricksField('client_id', { authentication_type: AUTH_TYPE_PAT })).toBe(false);
            expect(shouldShowDatabricksField('client_secret', { authentication_type: AUTH_TYPE_AZURE_AD })).toBe(false);
        });

        it('shows azure_auth fields for AZURE_AD, hides for others', () => {
            expect(shouldShowDatabricksField('azure_auth.tenant_id', { authentication_type: AUTH_TYPE_AZURE_AD })).toBe(
                true,
            );
            expect(shouldShowDatabricksField('azure_auth.client_id', { authentication_type: AUTH_TYPE_AZURE_AD })).toBe(
                true,
            );
            expect(
                shouldShowDatabricksField('azure_auth.client_secret', { authentication_type: AUTH_TYPE_AZURE_AD }),
            ).toBe(true);
            expect(shouldShowDatabricksField('azure_auth.tenant_id', { authentication_type: AUTH_TYPE_PAT })).toBe(
                false,
            );
        });

        it('shows non-auth fields regardless of auth type', () => {
            expect(shouldShowDatabricksField('workspace_url', { authentication_type: AUTH_TYPE_PAT })).toBe(true);
            expect(shouldShowDatabricksField('workspace_url', { authentication_type: AUTH_TYPE_OAUTH_M2M })).toBe(true);
        });
    });

    describe('backward compatibility (no authentication_type)', () => {
        it('shows token when token is set', () => {
            expect(shouldShowDatabricksField('token', { token: 'mytoken' })).toBe(true);
        });

        it('shows token by default when no credentials are set', () => {
            expect(shouldShowDatabricksField('token', {})).toBe(true);
        });

        it('hides token when oauth credentials are present', () => {
            expect(shouldShowDatabricksField('token', { client_id: 'abc', client_secret: 'xyz' })).toBe(false);
        });

        it('hides token when azure credentials are present', () => {
            expect(shouldShowDatabricksField('token', { 'azure_auth.tenant_id': 'tenant' })).toBe(false);
        });

        it('shows oauth fields when oauth credentials are set', () => {
            expect(shouldShowDatabricksField('client_id', { client_id: 'abc' })).toBe(true);
            expect(shouldShowDatabricksField('client_secret', { client_secret: 'xyz' })).toBe(true);
        });

        it('hides oauth fields when only a token is set', () => {
            expect(shouldShowDatabricksField('client_id', { token: 'mytoken' })).toBe(false);
        });

        it('shows azure fields when azure credentials are set via flat keys', () => {
            expect(shouldShowDatabricksField('azure_auth.tenant_id', { 'azure_auth.tenant_id': 'tenant' })).toBe(true);
        });

        it('shows azure fields when azure credentials are set via nested object', () => {
            expect(shouldShowDatabricksField('azure_auth.tenant_id', { azure_auth: { tenant_id: 'tenant' } })).toBe(
                true,
            );
        });

        it('hides azure fields when only oauth credentials are set', () => {
            expect(shouldShowDatabricksField('azure_auth.tenant_id', { client_id: 'abc' })).toBe(false);
        });
    });
});

describe('setDatabricksAuthTypeOnRecipe', () => {
    it('does NOT write authentication_type to the recipe (Python config rejects unknown fields)', () => {
        // Regression: UnityCatalogConnectionConfig has no `authentication_type` field
        // and inherits ConfigModel which uses extra="forbid", so writing it to the
        // recipe would itself fail validation.
        const result = setDatabricksAuthTypeOnRecipe(
            { source: { config: { workspace_url: 'wsurl' } } },
            AUTH_TYPE_OAUTH_M2M,
        );
        expect(result.source.config.authentication_type).toBeUndefined();
        expect(result.source.config.workspace_url).toBe('wsurl');
    });

    it('strips OAuth and Azure credentials when switching to PAT', () => {
        const recipe = {
            source: {
                config: {
                    token: 'oldtoken',
                    client_id: 'oauthid',
                    client_secret: 'oauthsecret',
                    azure_auth: { tenant_id: 't', client_id: 'c', client_secret: 's' },
                },
            },
        };
        const result = setDatabricksAuthTypeOnRecipe(recipe, AUTH_TYPE_PAT);
        expect(result.source.config.authentication_type).toBeUndefined();
        expect(result.source.config.token).toBe('oldtoken');
        expect(result.source.config.client_id).toBeUndefined();
        expect(result.source.config.client_secret).toBeUndefined();
        expect(result.source.config.azure_auth).toBeUndefined();
    });

    it('strips PAT and Azure credentials when switching to OAuth M2M', () => {
        const recipe = {
            source: {
                config: {
                    token: 'mytoken',
                    client_id: 'oauthid',
                    client_secret: 'oauthsecret',
                    azure_auth: { tenant_id: 't' },
                },
            },
        };
        const result = setDatabricksAuthTypeOnRecipe(recipe, AUTH_TYPE_OAUTH_M2M);
        expect(result.source.config.authentication_type).toBeUndefined();
        expect(result.source.config.token).toBeUndefined();
        expect(result.source.config.azure_auth).toBeUndefined();
        expect(result.source.config.client_id).toBe('oauthid');
        expect(result.source.config.client_secret).toBe('oauthsecret');
    });

    it('strips PAT and OAuth credentials when switching to Azure AD', () => {
        const recipe = {
            source: {
                config: {
                    token: 'mytoken',
                    client_id: 'oauthid',
                    client_secret: 'oauthsecret',
                    azure_auth: { tenant_id: 't', client_id: 'c', client_secret: 's' },
                },
            },
        };
        const result = setDatabricksAuthTypeOnRecipe(recipe, AUTH_TYPE_AZURE_AD);
        expect(result.source.config.authentication_type).toBeUndefined();
        expect(result.source.config.token).toBeUndefined();
        expect(result.source.config.client_id).toBeUndefined();
        expect(result.source.config.client_secret).toBeUndefined();
        expect(result.source.config.azure_auth).toEqual({ tenant_id: 't', client_id: 'c', client_secret: 's' });
    });

    it('strips a stale authentication_type that an earlier form version may have written', () => {
        const recipe = { source: { config: { authentication_type: AUTH_TYPE_PAT, token: 'tok' } } };
        const result = setDatabricksAuthTypeOnRecipe(recipe, AUTH_TYPE_PAT);
        expect(result.source.config.authentication_type).toBeUndefined();
        expect(result.source.config.token).toBe('tok');
    });

    it('strips all credentials when the auth type is cleared', () => {
        const recipe = {
            source: {
                config: { token: 't', client_id: 'c', client_secret: 's', azure_auth: { tenant_id: 'x' } },
            },
        };
        const result = setDatabricksAuthTypeOnRecipe(recipe, undefined);
        expect(result.source.config.token).toBeUndefined();
        expect(result.source.config.client_id).toBeUndefined();
        expect(result.source.config.client_secret).toBeUndefined();
        expect(result.source.config.azure_auth).toBeUndefined();
    });

    it('preserves unrelated config fields', () => {
        const recipe = {
            source: {
                config: {
                    workspace_url: 'https://my-org.cloud.databricks.com',
                    warehouse_id: 'wh-123',
                    token: 'mytoken',
                    include_table_lineage: true,
                },
            },
        };
        const result = setDatabricksAuthTypeOnRecipe(recipe, AUTH_TYPE_OAUTH_M2M);
        expect(result.source.config.workspace_url).toBe('https://my-org.cloud.databricks.com');
        expect(result.source.config.warehouse_id).toBe('wh-123');
        expect(result.source.config.include_table_lineage).toBe(true);
    });
});

describe('getDatabricksAuthTypeFromRecipe', () => {
    it('infers AZURE_AD when azure_auth is present', () => {
        const recipe = { source: { config: { azure_auth: { tenant_id: 't' } } } };
        expect(getDatabricksAuthTypeFromRecipe(recipe)).toBe(AUTH_TYPE_AZURE_AD);
    });

    it('infers OAUTH_M2M when only client_id/secret are present', () => {
        expect(getDatabricksAuthTypeFromRecipe({ source: { config: { client_id: 'c' } } })).toBe(AUTH_TYPE_OAUTH_M2M);
        expect(getDatabricksAuthTypeFromRecipe({ source: { config: { client_secret: 's' } } })).toBe(
            AUTH_TYPE_OAUTH_M2M,
        );
    });

    it('falls back to PAT when only a token is present', () => {
        expect(getDatabricksAuthTypeFromRecipe({ source: { config: { token: 'mytoken' } } })).toBe(AUTH_TYPE_PAT);
    });

    it('defaults to PAT for an empty recipe', () => {
        expect(getDatabricksAuthTypeFromRecipe({ source: { config: {} } })).toBe(AUTH_TYPE_PAT);
    });

    it('prioritizes Azure when both azure_auth and other creds are present', () => {
        // Picking the auth method that has the richest credential block is a useful
        // tiebreaker for legacy recipes that accidentally accumulated stale fields.
        const recipe = {
            source: { config: { azure_auth: { tenant_id: 't' }, client_id: 'leftover' } },
        };
        expect(getDatabricksAuthTypeFromRecipe(recipe)).toBe(AUTH_TYPE_AZURE_AD);
    });
});

describe('createDatabricksAuthValidator', () => {
    it('resolves when auth type matches and value is provided', async () => {
        const validator = createDatabricksAuthValidator(AUTH_TYPE_PAT, 'Token', 'PAT');
        const rule = validator({ getFieldValue: () => AUTH_TYPE_PAT });
        await expect(rule.validator({}, 'mytoken')).resolves.toBeUndefined();
    });

    it('rejects when auth type matches and value is empty', async () => {
        const validator = createDatabricksAuthValidator(AUTH_TYPE_PAT, 'Token', 'PAT');
        const rule = validator({ getFieldValue: () => AUTH_TYPE_PAT });
        await expect(rule.validator({}, '')).rejects.toThrow('Token is required for PAT authentication');
    });

    it('resolves when auth type does not match, even with no value', async () => {
        const validator = createDatabricksAuthValidator(AUTH_TYPE_PAT, 'Token', 'PAT');
        const rule = validator({ getFieldValue: () => AUTH_TYPE_OAUTH_M2M });
        await expect(rule.validator({}, '')).resolves.toBeUndefined();
    });
});
