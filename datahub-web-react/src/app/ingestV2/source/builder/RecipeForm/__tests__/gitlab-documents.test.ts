import { describe, expect, it } from 'vitest';

import { RECIPE_FIELDS } from '@app/ingestV2/source/builder/RecipeForm/constants';
import { GITLAB_DOCUMENTS } from '@app/ingestV2/source/builder/constants';
import sources from '@app/ingestV2/source/builder/sources.json';

describe('gitlab-documents recipe form', () => {
    it('is wired to the gitlab-documents source card', () => {
        expect(sources.some((source) => source.name === GITLAB_DOCUMENTS)).toBe(true);
        expect(RECIPE_FIELDS[GITLAB_DOCUMENTS]).toBeDefined();
    });

    it('writes every field to the gitlab-documents config, never the GitHub token', () => {
        const { fields, advancedFields } = RECIPE_FIELDS[GITLAB_DOCUMENTS];
        const fieldPaths = [...fields, ...advancedFields].map((field) => field.fieldPath);

        expect(fieldPaths).toEqual(
            expect.arrayContaining([
                'source.config.gitlab_token',
                'source.config.base_url',
                'source.config.repository',
                'source.config.branch',
                'source.config.document_import_mode',
                'source.config.create_repo_root_document',
            ]),
        );
        expect(fieldPaths).not.toContain('source.config.github_token');
    });
});
