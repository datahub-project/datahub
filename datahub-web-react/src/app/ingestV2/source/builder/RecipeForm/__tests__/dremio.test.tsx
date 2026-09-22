import { describe, expect, it } from 'vitest';

import {
    DREMIO_AUTHENTICATION_METHOD,
    DREMIO_USERNAME,
    getDremioAuthenticationMethodFromRecipe,
    setDremioAuthenticationMethodOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/dremio';

describe('setDremioAuthenticationMethodOnRecipe', () => {
    it('writes the new authentication_method into the recipe', () => {
        const result = setDremioAuthenticationMethodOnRecipe(
            { source: { config: { hostname: 'dremio.example.com' } } },
            'password',
        );
        expect(result.source.config.authentication_method).toBe('password');
        expect(result.source.config.hostname).toBe('dremio.example.com');
    });

    it('strips stale username when switching to PAT', () => {
        // Regression: PAT auth uses only the password field as a token; a leftover
        // username from a prior password-mode entry is irrelevant and surprised users
        // when it persisted in the YAML.
        const recipe = {
            source: {
                config: {
                    authentication_method: 'password',
                    username: 'old_user',
                    password: 'old_password',
                },
            },
        };
        const result = setDremioAuthenticationMethodOnRecipe(recipe, 'PAT');

        expect(result.source.config.authentication_method).toBe('PAT');
        expect(result.source.config.username).toBeUndefined();
        // Password is reused as the PAT — we don't touch it.
        expect(result.source.config.password).toBe('old_password');
    });

    it('keeps username when switching to password auth', () => {
        const recipe = {
            source: {
                config: { authentication_method: 'PAT', username: 'real_user', password: 'tok' },
            },
        };
        const result = setDremioAuthenticationMethodOnRecipe(recipe, 'password');

        expect(result.source.config.authentication_method).toBe('password');
        expect(result.source.config.username).toBe('real_user');
    });

    it('preserves unrelated config fields', () => {
        const recipe = { source: { config: { hostname: 'h', port: 9047, tls: true, username: 'u' } } };
        const result = setDremioAuthenticationMethodOnRecipe(recipe, 'PAT');
        expect(result.source.config.hostname).toBe('h');
        expect(result.source.config.port).toBe(9047);
        expect(result.source.config.tls).toBe(true);
    });
});

describe('getDremioAuthenticationMethodFromRecipe', () => {
    it('returns the explicit authentication_method when set', () => {
        expect(
            getDremioAuthenticationMethodFromRecipe({ source: { config: { authentication_method: 'password' } } }),
        ).toBe('password');
    });

    it('infers password when username is present but auth method is missing', () => {
        expect(getDremioAuthenticationMethodFromRecipe({ source: { config: { username: 'u' } } })).toBe('password');
    });

    it('defaults to PAT for an empty recipe', () => {
        expect(getDremioAuthenticationMethodFromRecipe({ source: { config: {} } })).toBe('PAT');
    });
});

describe('DREMIO_USERNAME visibility', () => {
    it('is hidden when authentication_method is PAT', () => {
        expect(DREMIO_USERNAME.dynamicHidden?.({ authentication_method: 'PAT' })).toBe(true);
    });

    it('is visible when authentication_method is password', () => {
        expect(DREMIO_USERNAME.dynamicHidden?.({ authentication_method: 'password' })).toBe(false);
    });

    it('is visible when authentication_method is undefined (form leaves the default unresolved)', () => {
        expect(DREMIO_USERNAME.dynamicHidden?.({})).toBe(false);
    });
});

describe('DREMIO_AUTHENTICATION_METHOD wiring', () => {
    it('uses the cleanup helper for setValueOnRecipeOverride', () => {
        expect(DREMIO_AUTHENTICATION_METHOD.setValueOnRecipeOverride).toBe(setDremioAuthenticationMethodOnRecipe);
    });

    it('uses the inference helper for getValueFromRecipeOverride', () => {
        expect(DREMIO_AUTHENTICATION_METHOD.getValueFromRecipeOverride).toBe(getDremioAuthenticationMethodFromRecipe);
    });
});
