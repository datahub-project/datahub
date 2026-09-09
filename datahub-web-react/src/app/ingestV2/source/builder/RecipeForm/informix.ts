import { get } from 'lodash';

import { FieldType, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

export const INFORMIX_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host and Port',
    helper: 'Informix host and port',
    tooltip:
        "The host and port where Informix is running. For example, 'informix:9088'. Note: this host must be accessible on the network where DataHub is running (or allowed via an IP Allow List, AWS PrivateLink, etc).",
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    placeholder: 'informix:9088',
    required: true,
    rules: null,
};

export const INFORMIX_SERVER: RecipeField = {
    name: 'server',
    label: 'Informix Server',
    helper: 'INFORMIXSERVER name',
    tooltip: 'The Informix server name (INFORMIXSERVER) used in the JDBC connection URL.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.server',
    placeholder: 'informix',
    required: true,
    rules: null,
};

export const INFORMIX_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database',
    helper: 'Specific Database to ingest',
    tooltip: 'Ingest metadata for a specific Informix database.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.database',
    placeholder: 'stores_demo',
    required: true,
    rules: null,
};

export const INFORMIX_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    helper: 'Informix username for metadata',
    tooltip: 'The Informix username used to extract metadata.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'informix',
    required: true,
    rules: null,
};

export const INFORMIX_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    helper: 'Informix password for user',
    tooltip: 'The Informix password for the user.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'password',
    required: true,
    rules: null,
};

export const INFORMIX_ACCEPT_IBM_JDBC_LICENSE: RecipeField = {
    name: 'accept_ibm_jdbc_license',
    label: 'Accept IBM JDBC License',
    helper: 'Allow download of IBM Informix JDBC driver',
    tooltip:
        'Required to auto-download the proprietary IBM Informix JDBC driver from Maven Central under the IBM Informix JDBC Software License Agreement. Not needed when driver_jar_paths is set via YAML.',
    type: FieldType.BOOLEAN,
    fieldPath: 'source.config.accept_ibm_jdbc_license',
    required: true,
    rules: null,
};

const includeViewLineagePath = 'source.config.include_view_lineage';
export const INFORMIX_INCLUDE_VIEW_LINEAGE: RecipeField = {
    name: 'include_view_lineage',
    label: 'Include View Lineage',
    helper: 'Extract view lineage from source',
    tooltip: 'Extract table- and column-level lineage for views by parsing their SQL definitions.',
    type: FieldType.BOOLEAN,
    fieldPath: includeViewLineagePath,
    getValueFromRecipeOverride: (recipe: any) => {
        const includeViewLineage = get(recipe, includeViewLineagePath);
        if (includeViewLineage !== undefined && includeViewLineage !== null) {
            return includeViewLineage;
        }
        return true;
    },
    rules: null,
};
