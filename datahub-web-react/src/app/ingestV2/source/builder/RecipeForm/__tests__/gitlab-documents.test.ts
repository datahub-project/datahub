import { describe, expect, it } from 'vitest';

import { RECIPE_FIELDS } from '@app/ingestV2/source/builder/RecipeForm/constants';
import {
    GITLAB_DOCUMENTS_BRANCH,
    GITLAB_DOCUMENTS_REPOSITORIES,
} from '@app/ingestV2/source/builder/RecipeForm/gitlab-documents';
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
                'source.config.repositories',
                'source.config.branch',
                'source.config.document_import_mode',
                'source.config.create_repo_root_document',
            ]),
        );
        expect(fieldPaths).not.toContain('source.config.github_token');
    });

    it('moves a project from the deprecated repository field into repositories', () => {
        const legacy = { source: { config: { repository: 'acme/docs', branch: 'main' } } };
        const projects = GITLAB_DOCUMENTS_REPOSITORIES.getValueFromRecipeOverride?.(legacy);
        expect(projects).toEqual(['acme/docs']);

        const saved = GITLAB_DOCUMENTS_REPOSITORIES.setValueOnRecipeOverride?.(legacy, [...projects, 'acme/handbook']);
        expect(saved.source.config.repositories).toEqual(['acme/docs', 'acme/handbook']);
        expect(saved.source.config.repository).toBeUndefined();
        expect(saved.source.config.branch).toBe('main');
    });

    it('rejects an empty or blank project list, since item rules never run for an empty list', async () => {
        const [rule] = GITLAB_DOCUMENTS_REPOSITORIES.rules as any[];
        await expect(rule.validator({}, [])).rejects.toThrow('Projects is required');
        await expect(rule.validator({}, undefined)).rejects.toThrow('Projects is required');
        await expect(rule.validator({}, ['  '])).rejects.toThrow('Projects is required');
        await expect(rule.validator({}, ['acme/docs'])).resolves.toBeUndefined();
    });

    it('rewrites the legacy repository key on every change, not only when projects change', () => {
        expect(GITLAB_DOCUMENTS_REPOSITORIES.migrateOnEveryChange).toBe(true);
    });

    it('leaves branch optional so each project uses its default branch', () => {
        expect(GITLAB_DOCUMENTS_BRANCH.required).toBeFalsy();
    });
});
