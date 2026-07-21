import { DATASET_ALLOW, DATASET_DENY } from '@app/ingestV2/source/builder/RecipeForm/bigqueryBeta';
import { FieldType } from '@app/ingestV2/source/builder/RecipeForm/common';
import { RECIPE_FIELDS } from '@app/ingestV2/source/builder/RecipeForm/constants';
import {
    ODCS_AWS_ACCESS_KEY_ID,
    ODCS_AWS_REGION,
    ODCS_AWS_SECRET_ACCESS_KEY,
    ODCS_GCS_HMAC_KEY_ID,
    ODCS_GCS_HMAC_KEY_SECRET,
    ODCS_GIT_INFO_BRANCH,
    ODCS_GIT_INFO_DEPLOY_KEY,
    ODCS_GIT_INFO_REPO,
    ODCS_LOCATION_GCS,
    ODCS_LOCATION_GIT,
    ODCS_LOCATION_HTTP,
    ODCS_LOCATION_LOCAL,
    ODCS_LOCATION_S3,
    ODCS_PATH,
    ODCS_SCHEMA_ASSERTION_COMPATIBILITY,
    ODCS_SOURCE_LOCATION,
} from '@app/ingestV2/source/builder/RecipeForm/odcs';
import { ODCS } from '@app/ingestV2/source/conf/odcs/odcs';

describe('ODCS recipe form fields', () => {
    it('registers ODCS in RECIPE_FIELDS with path first and the source-location selector in the connection section', () => {
        const spec = RECIPE_FIELDS[ODCS];
        expect(spec).toBeDefined();
        expect(spec.fields[0]).toBe(ODCS_PATH);
        expect(spec.fields).toContain(ODCS_SOURCE_LOCATION);
        expect(ODCS_PATH.required).toBe(true);
    });

    it('maps every field to a source.config.* recipe path matching its name', () => {
        // A typo'd fieldPath silently writes the wrong recipe key, so pin the contract explicitly.
        const spec = RECIPE_FIELDS[ODCS];
        const allFields = [...spec.fields, ...spec.advancedFields];
        allFields.forEach((field) => {
            const fieldPath = Array.isArray(field.fieldPath) ? field.fieldPath[0] : field.fieldPath;
            expect(fieldPath.startsWith('source.config.')).toBe(true);
        });
        expect(ODCS_PATH.fieldPath).toBe('source.config.path');
    });

    it('offers exactly the schema-assertion compatibility modes the connector accepts', () => {
        expect(ODCS_SCHEMA_ASSERTION_COMPATIBILITY.type).toBe(FieldType.SELECT);
        const values = (ODCS_SCHEMA_ASSERTION_COMPATIBILITY.options ?? []).map((o) => o.value);
        expect(values).toEqual(['SUPERSET', 'EXACT_MATCH', 'SUBSET']);
    });

    it('exposes remote-sourcing fields (git, S3, GCS) in the connection section wired to the matching config keys', () => {
        const { fields } = RECIPE_FIELDS[ODCS];
        [
            ODCS_GIT_INFO_REPO,
            ODCS_GIT_INFO_BRANCH,
            ODCS_GIT_INFO_DEPLOY_KEY,
            ODCS_AWS_ACCESS_KEY_ID,
            ODCS_AWS_SECRET_ACCESS_KEY,
            ODCS_AWS_REGION,
            ODCS_GCS_HMAC_KEY_ID,
            ODCS_GCS_HMAC_KEY_SECRET,
        ].forEach((field) => expect(fields).toContain(field));

        expect(ODCS_GIT_INFO_REPO.fieldPath).toBe('source.config.git_info.repo');
        expect(ODCS_AWS_ACCESS_KEY_ID.fieldPath).toBe('source.config.aws_connection.aws_access_key_id');
        expect(ODCS_AWS_SECRET_ACCESS_KEY.fieldPath).toBe('source.config.aws_connection.aws_secret_access_key');
        expect(ODCS_AWS_REGION.fieldPath).toBe('source.config.aws_connection.aws_region');
        expect(ODCS_GCS_HMAC_KEY_ID.fieldPath).toBe('source.config.gcs_connection.credential.hmac_access_id');
        expect(ODCS_GCS_HMAC_KEY_SECRET.fieldPath).toBe('source.config.gcs_connection.credential.hmac_access_secret');
    });

    it('exposes dataset_pattern allow/deny filters wired to the connector config', () => {
        const { filterFields } = RECIPE_FIELDS[ODCS];
        expect(filterFields).toEqual([DATASET_ALLOW, DATASET_DENY]);
        expect(DATASET_ALLOW.fieldPath).toBe('source.config.dataset_pattern.allow');
        expect(DATASET_DENY.fieldPath).toBe('source.config.dataset_pattern.deny');
    });

    it('treats the git deploy key and object-store secrets as secrets', () => {
        expect(ODCS_GIT_INFO_DEPLOY_KEY.type).toBe(FieldType.SECRET);
        expect(ODCS_AWS_SECRET_ACCESS_KEY.type).toBe(FieldType.SECRET);
        expect(ODCS_GCS_HMAC_KEY_SECRET.type).toBe(FieldType.SECRET);
    });

    it('is a single-select offering all five source-location options', () => {
        expect(ODCS_SOURCE_LOCATION.type).toBe(FieldType.SELECT);
        const values = (ODCS_SOURCE_LOCATION.options ?? []).map((o) => o.value);
        expect(values).toEqual([
            ODCS_LOCATION_LOCAL,
            ODCS_LOCATION_S3,
            ODCS_LOCATION_GCS,
            ODCS_LOCATION_HTTP,
            ODCS_LOCATION_GIT,
        ]);
    });

    it('reveals only the selected location credential fields', () => {
        const shown = (location: string | undefined, field: typeof ODCS_AWS_REGION) =>
            !(field.dynamicHidden?.({ source_location: location }) ?? false);

        // S3 reveals all three S3 fields and nothing else.
        expect(shown(ODCS_LOCATION_S3, ODCS_AWS_ACCESS_KEY_ID)).toBe(true);
        expect(shown(ODCS_LOCATION_S3, ODCS_AWS_SECRET_ACCESS_KEY)).toBe(true);
        expect(shown(ODCS_LOCATION_S3, ODCS_AWS_REGION)).toBe(true);
        expect(shown(ODCS_LOCATION_S3, ODCS_GCS_HMAC_KEY_ID)).toBe(false);
        expect(shown(ODCS_LOCATION_S3, ODCS_GIT_INFO_REPO)).toBe(false);

        // Git reveals the git fields only.
        expect(shown(ODCS_LOCATION_GIT, ODCS_GIT_INFO_REPO)).toBe(true);
        expect(shown(ODCS_LOCATION_GIT, ODCS_GIT_INFO_DEPLOY_KEY)).toBe(true);
        expect(shown(ODCS_LOCATION_GIT, ODCS_AWS_REGION)).toBe(false);

        // Local / HTTP / unset reveal no credential fields.
        [ODCS_AWS_REGION, ODCS_GCS_HMAC_KEY_ID, ODCS_GCS_HMAC_KEY_SECRET, ODCS_GIT_INFO_REPO].forEach((field) => {
            expect(shown(ODCS_LOCATION_LOCAL, field)).toBe(false);
            expect(shown(ODCS_LOCATION_HTTP, field)).toBe(false);
            expect(shown(undefined, field)).toBe(false);
        });
    });

    it('marks a credential field required only when its location is selected', () => {
        expect(ODCS_AWS_REGION.dynamicRequired?.({ source_location: ODCS_LOCATION_S3 })).toBe(true);
        expect(ODCS_AWS_REGION.dynamicRequired?.({ source_location: ODCS_LOCATION_GCS })).toBe(false);
        expect(ODCS_GCS_HMAC_KEY_ID.dynamicRequired?.({ source_location: ODCS_LOCATION_GCS })).toBe(true);
        expect(ODCS_GIT_INFO_REPO.dynamicRequired?.({ source_location: ODCS_LOCATION_GIT })).toBe(true);
    });

    it('never writes the form-only selector to the recipe and drops the other locations credentials', () => {
        const recipe = {
            source: {
                config: {
                    path: 's3://bucket/contracts/*.odcs.yaml',
                    aws_connection: { aws_region: 'us-east-1' },
                    gcs_connection: { credential: { hmac_access_id: 'x', hmac_access_secret: 'y' } },
                    git_info: { repo: 'org/repo' },
                },
            },
        };
        const updated = ODCS_SOURCE_LOCATION.setValueOnRecipeOverride?.(recipe, ODCS_LOCATION_S3);
        expect(updated.source.config.source_location).toBeUndefined();
        expect(updated.source.config.aws_connection).toBeDefined();
        expect(updated.source.config.gcs_connection).toBeUndefined();
        expect(updated.source.config.git_info).toBeUndefined();
    });

    it('infers the source location when editing a recipe authored in YAML', () => {
        const infer = (config: object) => ODCS_SOURCE_LOCATION.getValueFromRecipeOverride?.({ source: { config } });
        expect(infer({ path: 's3://b/x.odcs.yaml' })).toBe(ODCS_LOCATION_S3);
        expect(infer({ path: 'gs://b/x.odcs.yaml' })).toBe(ODCS_LOCATION_GCS);
        expect(infer({ path: 'https://host/x.odcs.yaml' })).toBe(ODCS_LOCATION_HTTP);
        expect(infer({ path: '/local/contracts' })).toBe(ODCS_LOCATION_LOCAL);
        // A configured git repo wins regardless of the path scheme.
        expect(infer({ path: ['contracts/', 's3://b/x.odcs.yaml'], git_info: { repo: 'org/repo' } })).toBe(
            ODCS_LOCATION_GIT,
        );
    });
});
