import { describe, expect, it } from 'vitest';

import { LOOKML_GIT_INFO_DEPLOY_KEY, LOOKML_GIT_INFO_REPO } from '@app/ingest/source/builder/RecipeForm/lookml';

describe('OSS LookML git_info migration', () => {
    it('LOOKML_GIT_INFO_REPO uses the modern git_info.repo path', () => {
        // Regression: the form used to write to `github_info.repo`, the deprecated
        // path that the Python connector accepts only via a renamed-field shim.
        expect(LOOKML_GIT_INFO_REPO.fieldPath).toBe('source.config.git_info.repo');
    });

    it('LOOKML_GIT_INFO_DEPLOY_KEY uses the modern git_info.deploy_key path', () => {
        expect(LOOKML_GIT_INFO_DEPLOY_KEY.fieldPath).toBe('source.config.git_info.deploy_key');
    });

    it('falls back to the legacy github_info.repo when editing an old recipe', () => {
        const legacyRecipe = { source: { config: { github_info: { repo: 'old-org/old-repo' } } } };
        expect(LOOKML_GIT_INFO_REPO.getValueFromRecipeOverride?.(legacyRecipe)).toBe('old-org/old-repo');
    });

    it('falls back to the legacy github_info.deploy_key when editing an old recipe', () => {
        const legacyRecipe = {
            source: { config: { github_info: { deploy_key: 'legacy-deploy-key-placeholder' } } },
        };
        expect(LOOKML_GIT_INFO_DEPLOY_KEY.getValueFromRecipeOverride?.(legacyRecipe)).toBe(
            'legacy-deploy-key-placeholder',
        );
    });

    it('prefers the modern path when both are present', () => {
        const recipe = {
            source: { config: { git_info: { repo: 'new/repo' }, github_info: { repo: 'old/repo' } } },
        };
        expect(LOOKML_GIT_INFO_REPO.getValueFromRecipeOverride?.(recipe)).toBe('new/repo');
    });

    it('save writes the modern path and removes the legacy github_info block', () => {
        // Avoids leaving both keys in the YAML, which the renamed-field shim treats
        // as ambiguous.
        const recipe = { source: { config: { github_info: { repo: 'old/repo' } } } };
        const result = LOOKML_GIT_INFO_REPO.setValueOnRecipeOverride?.(recipe, 'new/repo');
        expect(result.source.config.git_info.repo).toBe('new/repo');
        expect(result.source.config.github_info).toBeUndefined();
    });

    it('save appends a newline to the deploy key and removes the legacy github_info block', () => {
        const recipe = { source: { config: { github_info: { deploy_key: 'old' } } } };
        const result = LOOKML_GIT_INFO_DEPLOY_KEY.setValueOnRecipeOverride?.(recipe, 'newkey');
        expect(result.source.config.git_info.deploy_key).toBe('newkey\n');
        expect(result.source.config.github_info).toBeUndefined();
    });

    it('carries over sibling github_info keys (e.g. deploy_key) when only repo is edited', () => {
        // Without the migrator, editing the repo would silently strip the legacy
        // deploy_key on save.
        const recipe = {
            source: { config: { github_info: { repo: 'old/repo', deploy_key: 'oldkey', branch: 'main' } } },
        };
        const result = LOOKML_GIT_INFO_REPO.setValueOnRecipeOverride?.(recipe, 'new/repo');
        expect(result.source.config.git_info.repo).toBe('new/repo');
        expect(result.source.config.git_info.deploy_key).toBe('oldkey');
        expect(result.source.config.git_info.branch).toBe('main');
        expect(result.source.config.github_info).toBeUndefined();
    });

    it('the modern git_info values win on collision during migration', () => {
        // If a partly-migrated recipe has both, never undo the user's modern value
        // with whatever was left in the legacy block.
        const recipe = {
            source: {
                config: {
                    git_info: { repo: 'modern/repo' },
                    github_info: { repo: 'legacy/repo', deploy_key: 'legacy_key' },
                },
            },
        };
        const result = LOOKML_GIT_INFO_REPO.setValueOnRecipeOverride?.(recipe, 'modern/repo');
        expect(result.source.config.git_info.repo).toBe('modern/repo');
        expect(result.source.config.git_info.deploy_key).toBe('legacy_key');
        expect(result.source.config.github_info).toBeUndefined();
    });
});
