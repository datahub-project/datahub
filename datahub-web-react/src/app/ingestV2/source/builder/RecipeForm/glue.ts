import { get, omit } from 'lodash';

import {
    FieldType,
    FieldsValues,
    PROFILING_ENABLED,
    REMOVE_STALE_METADATA_ENABLED,
    RecipeField,
} from '@app/ingestV2/source/builder/RecipeForm/common';

export const GLUE = 'glue';

const awsRegionFieldPath = 'source.config.aws_region';
export const GLUE_AWS_REGION: RecipeField = {
    name: 'aws_region',
    label: 'AWS Region',
    tooltip: 'AWS region where Glue catalog lives (e.g. us-east-1, eu-west-1). Required for API access.',
    type: FieldType.TEXT,
    fieldPath: awsRegionFieldPath,
    placeholder: 'us-east-1',
    required: true,
    rules: null,
};

// Constants for AWS Authorization
export const awsAuthMethodFieldName = 'aws_auth_method';
export const awsAuthAccessKeys = 'access_keys';
export const awsAuthIamRole = 'iam_role';
export const awsAuthDefaultCredentials = 'default_credentials';
export const awsAuthTypeFieldPath = 'source.config.aws_auth_method';
export const awsAccessKeyIdFieldPath = 'source.config.aws_access_key_id';
export const awsSecretAccessKeyFieldPath = 'source.config.aws_secret_access_key';
export const awsSessionTokenFieldPath = 'source.config.aws_session_token';
export const awsRoleFieldPath = 'source.config.aws_role';

export function setGlueAwsAuthMethodOnRecipe(recipe: any): any {
    let updatedRecipe = { ...recipe };
    const authType = get(updatedRecipe, awsAuthTypeFieldPath);

    const accessKeyFields = [awsAccessKeyIdFieldPath, awsSecretAccessKeyFieldPath, awsSessionTokenFieldPath];

    const roleFields = [awsRoleFieldPath];

    if (authType === awsAuthAccessKeys) {
        updatedRecipe = omit(updatedRecipe, accessKeyFields);
    } else if (authType === awsAuthIamRole) {
        updatedRecipe = omit(updatedRecipe, roleFields);
    } else {
        updatedRecipe = omit(updatedRecipe, [...accessKeyFields, ...roleFields]);
    }

    return updatedRecipe;
}

export function getGlueAwsAuthMethodFromRecipe(recipe: any): string {
    const isAwsAccessKeyIdFilled = !!get(recipe, awsAccessKeyIdFieldPath);
    const isAwsSecretAccessKeyFilled = !!get(recipe, awsSecretAccessKeyFieldPath);
    const isAwsRoleFilled = !!get(recipe, awsRoleFieldPath);

    if (isAwsAccessKeyIdFilled || isAwsSecretAccessKeyFilled) {
        return awsAuthAccessKeys;
    }
    if (isAwsRoleFilled) {
        return awsAuthIamRole;
    }
    return awsAuthDefaultCredentials;
}

export const GLUE_AWS_AUTHORIZATION_METHOD: RecipeField = {
    name: awsAuthMethodFieldName,
    label: 'AWS Authorization Method',
    tooltip: 'Select your AWS authentication method.',
    type: FieldType.SELECT,
    options: [
        { label: 'AWS Access Keys', value: awsAuthAccessKeys },
        { label: 'IAM Role', value: awsAuthIamRole },
        { label: 'Default Credentials', value: awsAuthDefaultCredentials },
    ],
    fieldPath: awsAuthTypeFieldPath,
    required: true,
    rules: null,
    setValueOnRecipeOverride: setGlueAwsAuthMethodOnRecipe,
    getValueFromRecipeOverride: getGlueAwsAuthMethodFromRecipe,
};

export const GLUE_AWS_ACCESS_KEY_ID: RecipeField = {
    name: 'aws_access_key_id',
    label: 'AWS Access Key ID',
    tooltip: 'AWS access key ID for authentication. For cross-account access use IAM role instead.',
    type: FieldType.SECRET,
    fieldPath: awsAccessKeyIdFieldPath,
    placeholder: 'your-aws-access-key-id',
    dynamicRequired: (values: FieldsValues) => get(values, awsAuthMethodFieldName) === awsAuthAccessKeys,
    dynamicHidden: (values: FieldsValues) => get(values, awsAuthMethodFieldName) !== awsAuthAccessKeys,
    rules: null,
};

export const GLUE_AWS_SECRET_ACCESS_KEY: RecipeField = {
    name: 'aws_secret_access_key',
    label: 'AWS Secret Access Key',
    tooltip: 'AWS secret access key paired with access key ID.',
    type: FieldType.SECRET,
    fieldPath: awsSecretAccessKeyFieldPath,
    placeholder: 'your-aws-secret-access-key',
    dynamicRequired: (values: FieldsValues) => get(values, awsAuthMethodFieldName) === awsAuthAccessKeys,
    dynamicHidden: (values: FieldsValues) => get(values, awsAuthMethodFieldName) !== awsAuthAccessKeys,
    rules: null,
};

export const GLUE_AWS_SESSION_TOKEN: RecipeField = {
    name: 'aws_session_token',
    label: 'AWS Session Token',
    tooltip: 'AWS session token for temporary credentials. Used with access key ID and secret.',
    type: FieldType.SECRET,
    fieldPath: awsSessionTokenFieldPath,
    placeholder: 'your-aws-session-token',
    dynamicHidden: (values: FieldsValues) => get(values, awsAuthMethodFieldName) !== awsAuthAccessKeys,
    rules: null,
};

export const GLUE_AWS_ROLE: RecipeField = {
    name: 'aws_role',
    label: 'IAM Role ARN',
    tooltip: 'IAM role ARN to assume for cross-account access. Format: arn:aws:iam::123456789012:role/RoleName.',
    type: FieldType.TEXT,
    fieldPath: awsRoleFieldPath,
    placeholder: 'arn:aws:iam::123:role/DataHubRole',
    dynamicRequired: (values: FieldsValues) => get(values, awsAuthMethodFieldName) === awsAuthIamRole,
    dynamicHidden: (values: FieldsValues) => get(values, awsAuthMethodFieldName) !== awsAuthIamRole,
    rules: null,
};

const catalogIdFieldPath = 'source.config.catalog_id';
export const GLUE_CATALOG_ID: RecipeField = {
    name: 'catalog_id',
    label: 'Catalog ID',
    tooltip:
        'AWS account ID of Glue catalog. Leave empty for same-account ingestion. Required for cross-account access.',
    type: FieldType.TEXT,
    fieldPath: catalogIdFieldPath,
    placeholder: '123456789012',
    rules: null,
};

const platformInstanceFieldPath = 'source.config.platform_instance';
export const GLUE_PLATFORM_INSTANCE: RecipeField = {
    name: 'platform_instance',
    label: 'Platform Instance',
    tooltip: 'Optional instance identifier (e.g. prod_glue). Critical for multi-database deployments and lineage.',
    type: FieldType.TEXT,
    fieldPath: platformInstanceFieldPath,
    placeholder: 'prod-glue',
    rules: null,
};

const extractOwnersFieldPath = 'source.config.extract_owners';
export const GLUE_EXTRACT_OWNERS: RecipeField = {
    name: 'extract_owners',
    label: 'Extract Owners',
    tooltip:
        'Import table owners from Glue Owner property. Will overwrite owners manually set in DataHub. Disable if managing ownership in DataHub.',
    type: FieldType.BOOLEAN,
    fieldPath: extractOwnersFieldPath,
    rules: null,
};

const extractTransformsFieldPath = 'source.config.extract_transforms';
export const GLUE_EXTRACT_TRANSFORMS: RecipeField = {
    name: 'extract_transforms',
    label: 'Extract ETL Jobs',
    tooltip: 'Extract Glue ETL jobs as DataFlows with lineage. Shows Glue job dependencies and transformations.',
    type: FieldType.BOOLEAN,
    fieldPath: extractTransformsFieldPath,
    rules: null,
};

const emitS3LineageFieldPath = 'source.config.emit_s3_lineage';
const emitS3LineageFieldName = 'emit_s3_lineage';
export const GLUE_EMIT_S3_LINEAGE: RecipeField = {
    name: emitS3LineageFieldName,
    label: 'Emit S3 Lineage',
    tooltip: 'Extract lineage between Glue tables and S3 locations. Shows S3-to-Glue or Glue-to-S3 data flow.',
    type: FieldType.BOOLEAN,
    fieldPath: emitS3LineageFieldPath,
    rules: null,
};

const includeColumnLineageFieldPath = 'source.config.include_column_lineage';
export const GLUE_INCLUDE_COLUMN_LINEAGE: RecipeField = {
    name: 'include_column_lineage',
    label: 'Column Lineage',
    tooltip: 'Extract column-level lineage from S3. Requires Emit S3 Lineage enabled.',
    type: FieldType.BOOLEAN,
    dynamicDisabled: (values) => !!get(values, emitS3LineageFieldName) !== true,
    fieldPath: includeColumnLineageFieldPath,
    rules: null,
};

export const GLUE_PROFILING_ENABLED: RecipeField = {
    ...PROFILING_ENABLED,
    tooltip:
        'Run profiling queries for table statistics. ⚠️ Queries actual data in S3 - may incur costs! Significantly increases ingestion time.',
};

export const GLUE_REMOVE_STALE_METADATA_ENABLED: RecipeField = {
    ...REMOVE_STALE_METADATA_ENABLED,
    tooltip:
        'Automatically remove deleted tables from DataHub. Maintains catalog accuracy when Glue tables are dropped.',
};
