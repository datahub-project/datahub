import { get } from 'lodash';
import { RecipeField, FieldType } from './common';

export const VERTICA_HOST_PORT: RecipeField = {
    name: 'host_port',
    label: 'Host and Port',
    tooltip:
        "The host and port where Vertica is running. For example, 'localhost:5433'. Note: this host must be accessible on the network where DataHub is running (or allowed via an IP Allow List, AWS PrivateLink, etc).",
    type: FieldType.TEXT,
    fieldPath: 'source.config.host_port',
    placeholder: 'localhost:5433',
    required: true,
    rules: null,
};

export const VERTICA_DATABASE: RecipeField = {
    name: 'database',
    label: 'Database',
    tooltip: 'Ingest metadata for a specific Database.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.database',
    placeholder: 'Vertica_Database',
    required: true,
    rules: null,
};

export const VERTICA_USERNAME: RecipeField = {
    name: 'username',
    label: 'Username',
    tooltip: 'The Vertica username used to extract metadata.',
    type: FieldType.TEXT,
    fieldPath: 'source.config.username',
    placeholder: 'Vertica_Username',
    required: true,
    rules: null,
};

export const VERTICA_PASSWORD: RecipeField = {
    name: 'password',
    label: 'Password',
    tooltip: 'The Vertica password for the user.',
    type: FieldType.SECRET,
    fieldPath: 'source.config.password',
    placeholder: 'Vertica_Password',
    required: true,
    rules: null,
};

const includeProjectionPath = 'source.config.include_projections';
export const INCLUDE_PROJECTIONS: RecipeField = {
    name: 'include_projections',
    label: 'Include Projections',
    tooltip: 'Extract Projections from source.',
    type: FieldType.BOOLEAN,
    fieldPath: includeProjectionPath,
    // This is in accordance with what the ingestion sources do.
    getValueFromRecipeOverride: (recipe: any) => {
        const includeProjection = get(recipe, includeProjectionPath);
        if (includeProjection !== undefined && includeProjection !== null) {
            return includeProjection;
        }
        return true;
    },
    rules: null,
};

const includemodelsPath = 'source.config.include_models';
export const INCLUDE_MLMODELS: RecipeField = {
    name: 'include_models',
    label: 'Include ML Models',
    tooltip: 'Extract ML models from source.',
    type: FieldType.BOOLEAN,
    fieldPath: includemodelsPath,
    // This is in accordance with what the ingestion sources do.
    getValueFromRecipeOverride: (recipe: any) => {
        const includeModel = get(recipe, includemodelsPath);
        if (includeModel !== undefined && includeModel !== null) {
            return includeModel;
        }
        return true;
    },
    rules: null,
};

const includeviewlineagePath = 'source.config.include_view_lineage';
export const INCLUDE_VIEW_LINEAGE: RecipeField = {
    name: 'include_view_lineage',
    label: 'Include View Lineage',
    tooltip: 'Extract View Lineage from source.',
    type: FieldType.BOOLEAN,
    fieldPath: includeviewlineagePath,
    // This is in accordance with what the ingestion sources do.
    getValueFromRecipeOverride: (recipe: any) => {
        const includeviewlineage = get(recipe, includeviewlineagePath);
        if (includeviewlineage !== undefined && includeviewlineage !== null) {
            return includeviewlineage;
        }
        return true;
    },
    rules: null,
};

const includeprojectionlineagePath = 'source.config.include_projection_lineage';
export const INCLUDE_PROJECTIONS_LINEAGE: RecipeField = {
    name: 'include_projection_lineage',
    label: 'Include Projection Lineage',
    tooltip: 'Extract Projection Lineage from source.',
    type: FieldType.BOOLEAN,
    fieldPath: includeprojectionlineagePath,
    // This is in accordance with what the ingestion sources do.
    getValueFromRecipeOverride: (recipe: any) => {
        const includeprojectionlineage = get(recipe, includeprojectionlineagePath);
        if (includeprojectionlineage !== undefined && includeprojectionlineage !== null) {
            return includeprojectionlineage;
        }
        return true;
    },
    rules: null,
};
