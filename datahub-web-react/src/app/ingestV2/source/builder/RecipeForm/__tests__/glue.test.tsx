import { describe, expect, it } from 'vitest';

import {
    awsAccessKeyIdFieldPath,
    awsAuthAccessKeys,
    awsAuthDefaultCredentials,
    awsAuthIamRole,
    awsAuthTypeFieldPath,
    awsRoleFieldPath,
    awsSecretAccessKeyFieldPath,
    awsSessionTokenFieldPath,
    getGlueAwsAuthMethodFromRecipe,
    setGlueAwsAuthMethodOnRecipe,
} from '@app/ingestV2/source/builder/RecipeForm/glue';

describe('getGlueAwsAuthMethodFromRecipe', () => {
    it('should return access_keys when access key ID is filled', () => {
        const recipe = {
            source: {
                config: {
                    aws_access_key_id: 'test-key-id',
                },
            },
        };
        const result = getGlueAwsAuthMethodFromRecipe(recipe);

        expect(result).toBe(awsAuthAccessKeys);
    });

    it('should return access_keys when secret access key is filled', () => {
        const recipe = {
            source: {
                config: {
                    aws_secret_access_key: 'test-secret',
                },
            },
        };
        const result = getGlueAwsAuthMethodFromRecipe(recipe);

        expect(result).toBe(awsAuthAccessKeys);
    });

    it('should return access_keys when both access key ID and secret are filled', () => {
        const recipe = {
            source: {
                config: {
                    aws_access_key_id: 'test-key-id',
                    aws_secret_access_key: 'test-secret',
                },
            },
        };
        const result = getGlueAwsAuthMethodFromRecipe(recipe);

        expect(result).toBe(awsAuthAccessKeys);
    });

    it('should return iam_role when role ARN is filled', () => {
        const recipe = {
            source: {
                config: {
                    aws_role: 'arn:aws:iam::123456789012:role/TestRole',
                },
            },
        };
        const result = getGlueAwsAuthMethodFromRecipe(recipe);

        expect(result).toBe(awsAuthIamRole);
    });

    it('should return default_credentials when no credentials are filled', () => {
        const recipe = { source: { config: {} } };
        const result = getGlueAwsAuthMethodFromRecipe(recipe);

        expect(result).toBe(awsAuthDefaultCredentials);
    });

    it('should prioritize access keys over IAM role', () => {
        const recipe = {
            source: {
                config: {
                    aws_access_key_id: 'test-key-id',
                    aws_role: 'arn:aws:iam::123456789012:role/TestRole',
                },
            },
        };
        const result = getGlueAwsAuthMethodFromRecipe(recipe);

        expect(result).toBe(awsAuthAccessKeys);
    });
});

describe('setGlueAwsAuthMethodOnRecipe', () => {
    it('writes the new auth method into the recipe and strips IAM role creds when switching to access_keys', () => {
        const recipe = {
            source: {
                config: {
                    aws_role: 'arn:aws:iam::123456789012:role/StaleRole',
                    aws_access_key_id: 'test-key-id',
                    aws_secret_access_key: 'test-secret',
                    aws_session_token: 'test-token',
                },
            },
        };
        const result = setGlueAwsAuthMethodOnRecipe(recipe, awsAuthAccessKeys);

        expect(result.source.config.aws_auth_method).toBeUndefined();
        // Access key creds should be preserved — this is the auth method we're using.
        expect(result.source.config.aws_access_key_id).toBe('test-key-id');
        expect(result.source.config.aws_secret_access_key).toBe('test-secret');
        expect(result.source.config.aws_session_token).toBe('test-token');
        // IAM role from a previous selection must be dropped.
        expect(result.source.config.aws_role).toBeUndefined();
    });

    it('writes the new auth method and strips access keys when switching to iam_role', () => {
        const recipe = {
            source: {
                config: {
                    aws_access_key_id: 'stale-key-id',
                    aws_secret_access_key: 'stale-secret',
                    aws_session_token: 'stale-token',
                    aws_role: 'arn:aws:iam::123456789012:role/TestRole',
                },
            },
        };
        const result = setGlueAwsAuthMethodOnRecipe(recipe, awsAuthIamRole);

        expect(result.source.config.aws_auth_method).toBeUndefined();
        expect(result.source.config.aws_role).toBe('arn:aws:iam::123456789012:role/TestRole');
        expect(result.source.config.aws_access_key_id).toBeUndefined();
        expect(result.source.config.aws_secret_access_key).toBeUndefined();
        expect(result.source.config.aws_session_token).toBeUndefined();
    });

    it('strips all credentials when switching to default_credentials', () => {
        const recipe = {
            source: {
                config: {
                    aws_access_key_id: 'test-key-id',
                    aws_secret_access_key: 'test-secret',
                    aws_session_token: 'test-token',
                    aws_role: 'arn:aws:iam::123456789012:role/TestRole',
                },
            },
        };
        const result = setGlueAwsAuthMethodOnRecipe(recipe, awsAuthDefaultCredentials);

        expect(result.source.config.aws_auth_method).toBeUndefined();
        expect(result.source.config.aws_access_key_id).toBeUndefined();
        expect(result.source.config.aws_secret_access_key).toBeUndefined();
        expect(result.source.config.aws_session_token).toBeUndefined();
        expect(result.source.config.aws_role).toBeUndefined();
    });

    it('strips a stale aws_auth_method from the recipe even though it is a UI-only field', () => {
        // Regression: aws_auth_method is not a real GlueSourceConfig field, and
        // ConfigModel uses extra="forbid" — leaving it in the recipe would cause the
        // connector to fail validation at ingestion time.
        const recipe = {
            source: { config: { aws_auth_method: awsAuthAccessKeys, aws_region: 'us-east-1' } },
        };
        const result = setGlueAwsAuthMethodOnRecipe(recipe, awsAuthIamRole);
        expect(result.source.config.aws_auth_method).toBeUndefined();
        expect(result.source.config.aws_region).toBe('us-east-1');
    });

    it('preserves unrelated config fields when toggling auth method', () => {
        const recipe = {
            source: {
                config: {
                    aws_region: 'us-east-1',
                    catalog_id: '123456789012',
                    aws_role: 'arn:aws:iam::123:role/Stale',
                },
            },
        };
        const result = setGlueAwsAuthMethodOnRecipe(recipe, awsAuthAccessKeys);

        expect(result.source.config.aws_region).toBe('us-east-1');
        expect(result.source.config.catalog_id).toBe('123456789012');
    });
});

describe('Field path constants', () => {
    it('should have correct field paths', () => {
        expect(awsAuthTypeFieldPath).toBe('source.config.aws_auth_method');
        expect(awsAccessKeyIdFieldPath).toBe('source.config.aws_access_key_id');
        expect(awsSecretAccessKeyFieldPath).toBe('source.config.aws_secret_access_key');
        expect(awsSessionTokenFieldPath).toBe('source.config.aws_session_token');
        expect(awsRoleFieldPath).toBe('source.config.aws_role');
    });

    it('should have correct auth type values', () => {
        expect(awsAuthAccessKeys).toBe('access_keys');
        expect(awsAuthIamRole).toBe('iam_role');
        expect(awsAuthDefaultCredentials).toBe('default_credentials');
    });
});
