import { describe, expect, it } from 'vitest';

import { LOOKML_GIT_INFO_DEPLOY_KEY, LOOKML_GIT_INFO_REPO } from '@app/ingest/source/builder/RecipeForm/lookml';

describe('LookML (legacy ingest) git_info fields', () => {
    describe('getValueFromRecipeOverride', () => {
        it('hydrates from a legacy github_info recipe', () => {
            const recipe = { source: { config: { github_info: { repo: 'acme/looker', deploy_key: 'legacy-key' } } } };
            expect(LOOKML_GIT_INFO_REPO.getValueFromRecipeOverride?.(recipe)).toBe('acme/looker');
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.getValueFromRecipeOverride?.(recipe)).toBe('legacy-key');
        });

        it('prefers the modern git_info block when both spellings are present', () => {
            const recipe = {
                source: {
                    config: {
                        git_info: { repo: 'acme/modern', deploy_key: 'modern-key' },
                        github_info: { repo: 'acme/legacy', deploy_key: 'legacy-key' },
                    },
                },
            };
            expect(LOOKML_GIT_INFO_REPO.getValueFromRecipeOverride?.(recipe)).toBe('acme/modern');
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.getValueFromRecipeOverride?.(recipe)).toBe('modern-key');
        });
    });

    describe('setValueOnRecipeOverride migrates the legacy block', () => {
        it('writes git_info and drops github_info entirely', () => {
            const recipe = { source: { config: { github_info: { repo: 'acme/legacy' } } } };
            const updated = LOOKML_GIT_INFO_REPO.setValueOnRecipeOverride?.(recipe, 'acme/looker');
            expect(updated.source.config.git_info).toEqual({ repo: 'acme/looker' });
            expect(updated.source.config.github_info).toBeUndefined();
        });

        it('keeps sibling keys the form never renders', () => {
            const recipe = {
                source: { config: { github_info: { repo: 'acme/legacy', branch: 'main', deploy_key: 'legacy-key' } } },
            };
            const updated = LOOKML_GIT_INFO_REPO.setValueOnRecipeOverride?.(recipe, 'acme/looker');
            // `branch` and `deploy_key` are not fields of this form, so a field-by-field
            // strip would silently lose them.
            expect(updated.source.config.git_info).toEqual({
                repo: 'acme/looker',
                branch: 'main',
                deploy_key: 'legacy-key',
            });
            expect(updated.source.config.github_info).toBeUndefined();
        });

        it('lets the value just typed win over the legacy one on collision', () => {
            const recipe = {
                source: {
                    config: {
                        git_info: { repo: 'acme/modern' },
                        github_info: { repo: 'acme/legacy', branch: 'main' },
                    },
                },
            };
            const updated = LOOKML_GIT_INFO_REPO.setValueOnRecipeOverride?.(recipe, 'acme/typed');
            expect(updated.source.config.git_info).toEqual({ repo: 'acme/typed', branch: 'main' });
            expect(updated.source.config.github_info).toBeUndefined();
        });

        it('appends the trailing newline the deploy key needs while migrating', () => {
            const recipe = { source: { config: { github_info: { repo: 'acme/legacy' } } } };
            const updated = LOOKML_GIT_INFO_DEPLOY_KEY.setValueOnRecipeOverride?.(recipe, 'ssh-key');
            expect(updated.source.config.git_info).toEqual({ repo: 'acme/legacy', deploy_key: 'ssh-key\n' });
            expect(updated.source.config.github_info).toBeUndefined();
        });
    });

    it('leaves a recipe with no legacy block untouched', () => {
        const recipe = { source: { config: {} } };
        const updated = LOOKML_GIT_INFO_REPO.setValueOnRecipeOverride?.(recipe, 'acme/looker');
        expect(updated.source.config.git_info).toEqual({ repo: 'acme/looker' });
        expect('github_info' in updated.source.config).toBe(false);
    });
});
