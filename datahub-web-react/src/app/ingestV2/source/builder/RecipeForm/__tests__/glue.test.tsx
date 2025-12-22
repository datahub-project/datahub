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
    it('should remove access key fields when auth type is access_keys', () => {
        const recipe = {
            source: {
                config: {
                    aws_auth_method: awsAuthAccessKeys,
                    aws_access_key_id: 'test-key-id',
                    aws_secret_access_key: 'test-secret',
                    aws_session_token: 'test-token',
                },
            },
        };
        const result = setGlueAwsAuthMethodOnRecipe(recipe);

        expect(result.source.config.aws_auth_method).toBe(awsAuthAccessKeys);
        expect(result.source.config.aws_access_key_id).toBeUndefined();
        expect(result.source.config.aws_secret_access_key).toBeUndefined();
        expect(result.source.config.aws_session_token).toBeUndefined();
    });

    it('should remove role field when auth type is iam_role', () => {
        const recipe = {
            source: {
                config: {
                    aws_auth_method: awsAuthIamRole,
                    aws_role: 'arn:aws:iam::123456789012:role/TestRole',
                },
            },
        };
        const result = setGlueAwsAuthMethodOnRecipe(recipe);

        expect(result.source.config.aws_auth_method).toBe(awsAuthIamRole);
        expect(result.source.config.aws_role).toBeUndefined();
    });

    it('should remove all credential fields when auth type is default_credentials', () => {
        const recipe = {
            source: {
                config: {
                    aws_auth_method: awsAuthDefaultCredentials,
                    aws_access_key_id: 'test-key-id',
                    aws_secret_access_key: 'test-secret',
                    aws_session_token: 'test-token',
                    aws_role: 'arn:aws:iam::123456789012:role/TestRole',
                },
            },
        };
        const result = setGlueAwsAuthMethodOnRecipe(recipe);

        expect(result.source.config.aws_auth_method).toBe(awsAuthDefaultCredentials);
        expect(result.source.config.aws_access_key_id).toBeUndefined();
        expect(result.source.config.aws_secret_access_key).toBeUndefined();
        expect(result.source.config.aws_session_token).toBeUndefined();
        expect(result.source.config.aws_role).toBeUndefined();
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
