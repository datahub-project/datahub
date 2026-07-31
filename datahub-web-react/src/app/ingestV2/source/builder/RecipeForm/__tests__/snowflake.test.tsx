import { describe, expect, it } from 'vitest';

import {
    SNOWFLAKE_AUTHENTICATION_TYPE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_PRIVATE_KEY,
    SNOWFLAKE_PRIVATE_KEY_PASSWORD,
} from '@app/ingestV2/source/builder/RecipeForm/snowflake';

describe('Snowflake authentication type helpers', () => {
    describe('setSnowflakeAuthTypeOnRecipe', () => {
        it('writes KEY_PAIR_AUTHENTICATOR into the recipe when Private Key is selected', () => {
            const recipe = {
                source: {
                    config: {
                        account_id: 'xyz123',
                    },
                },
            };
            const result = SNOWFLAKE_AUTHENTICATION_TYPE.setValueOnRecipeOverride?.(recipe, 'KEY_PAIR_AUTHENTICATOR');

            expect(result.source.config.authentication_type).toBe('KEY_PAIR_AUTHENTICATOR');
        });

        it('writes DEFAULT_AUTHENTICATOR into the recipe when Username & Password is selected', () => {
            const recipe = {
                source: {
                    config: {
                        account_id: 'xyz123',
                    },
                },
            };
            const result = SNOWFLAKE_AUTHENTICATION_TYPE.setValueOnRecipeOverride?.(recipe, 'DEFAULT_AUTHENTICATOR');

            expect(result.source.config.authentication_type).toBe('DEFAULT_AUTHENTICATOR');
        });

        it('drops password credentials when switching to key pair authentication', () => {
            const recipe = {
                source: {
                    config: {
                        password: 'secret',
                    },
                },
            };
            const result = SNOWFLAKE_AUTHENTICATION_TYPE.setValueOnRecipeOverride?.(recipe, 'KEY_PAIR_AUTHENTICATOR');

            expect(result.source.config.authentication_type).toBe('KEY_PAIR_AUTHENTICATOR');
            expect(result.source.config.password).toBeUndefined();
        });

        it('drops private key credentials when switching to username/password authentication', () => {
            const recipe = {
                source: {
                    config: {
                        private_key: '-----BEGIN PRIVATE KEY-----...',
                        private_key_password: 'keypass',
                    },
                },
            };
            const result = SNOWFLAKE_AUTHENTICATION_TYPE.setValueOnRecipeOverride?.(recipe, 'DEFAULT_AUTHENTICATOR');

            expect(result.source.config.authentication_type).toBe('DEFAULT_AUTHENTICATOR');
            expect(result.source.config.private_key).toBeUndefined();
            expect(result.source.config.private_key_password).toBeUndefined();
        });

        it('preserves unrelated config fields', () => {
            const recipe = {
                source: {
                    config: {
                        account_id: 'xyz123',
                        warehouse: 'COMPUTE_WH',
                        username: 'snowflake',
                    },
                },
            };
            const result = SNOWFLAKE_AUTHENTICATION_TYPE.setValueOnRecipeOverride?.(recipe, 'KEY_PAIR_AUTHENTICATOR');

            expect(result.source.config.account_id).toBe('xyz123');
            expect(result.source.config.warehouse).toBe('COMPUTE_WH');
            expect(result.source.config.username).toBe('snowflake');
        });
    });

    describe('getSnowflakeAuthTypeFromRecipe', () => {
        it('infers DEFAULT_AUTHENTICATOR when only a password is set', () => {
            const recipe = {
                source: {
                    config: {
                        password: 'secret',
                    },
                },
            };
            const result = SNOWFLAKE_AUTHENTICATION_TYPE.getValueFromRecipeOverride?.(recipe);

            expect(result).toBe('DEFAULT_AUTHENTICATOR');
        });

        it('defaults to KEY_PAIR_AUTHENTICATOR for a new (empty) recipe', () => {
            const recipe = { source: { config: {} } };
            const result = SNOWFLAKE_AUTHENTICATION_TYPE.getValueFromRecipeOverride?.(recipe);

            expect(result).toBe('KEY_PAIR_AUTHENTICATOR');
        });

        it('returns KEY_PAIR_AUTHENTICATOR when a private key is set', () => {
            const recipe = {
                source: {
                    config: {
                        private_key: '-----BEGIN PRIVATE KEY-----...',
                    },
                },
            };
            const result = SNOWFLAKE_AUTHENTICATION_TYPE.getValueFromRecipeOverride?.(recipe);

            expect(result).toBe('KEY_PAIR_AUTHENTICATOR');
        });
    });
});

describe('Snowflake credential field visibility', () => {
    it('hides the password field when authentication type is KEY_PAIR_AUTHENTICATOR', () => {
        const values = { authentication_type: 'KEY_PAIR_AUTHENTICATOR' };

        expect(SNOWFLAKE_PASSWORD.dynamicHidden?.(values)).toBe(true);
        expect(SNOWFLAKE_PRIVATE_KEY.dynamicHidden?.(values)).toBe(false);
        expect(SNOWFLAKE_PRIVATE_KEY_PASSWORD.dynamicHidden?.(values)).toBe(false);
    });

    it('hides the private key fields when authentication type is DEFAULT_AUTHENTICATOR', () => {
        const values = { authentication_type: 'DEFAULT_AUTHENTICATOR' };

        expect(SNOWFLAKE_PASSWORD.dynamicHidden?.(values)).toBe(false);
        expect(SNOWFLAKE_PRIVATE_KEY.dynamicHidden?.(values)).toBe(true);
        expect(SNOWFLAKE_PRIVATE_KEY_PASSWORD.dynamicHidden?.(values)).toBe(true);
    });
});
