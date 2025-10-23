import { GIT_INFO_REPO } from '@app/ingestV2/source/builder/RecipeForm/common';
import { RECIPE_FIELDS } from '@app/ingestV2/source/builder/RecipeForm/constants';
import { LOOKML, LOOKML_GIT_INFO_REPO } from '@app/ingestV2/source/builder/RecipeForm/lookml';

describe('Constants Git Info Integration', () => {
    describe('Field Imports', () => {
        it('should import LOOKML_GIT_INFO_REPO from lookml module', () => {
            expect(LOOKML_GIT_INFO_REPO).toBeDefined();
            expect(LOOKML_GIT_INFO_REPO.name).toBe('git_info.repo');
        });

        it('should import GIT_INFO_REPO from common module', () => {
            expect(GIT_INFO_REPO).toBeDefined();
            expect(GIT_INFO_REPO.name).toBe('git_info.repo');
        });
    });

    describe('Source Configuration Integration', () => {
        it('should include LOOKML_GIT_INFO_REPO in LOOKML source config', () => {
            const lookmlConfig = RECIPE_FIELDS[LOOKML];
            expect(lookmlConfig).toBeDefined();
            expect(lookmlConfig.fields).toContain(LOOKML_GIT_INFO_REPO);
        });

        it('should have correct field order in LOOKML config', () => {
            const lookmlConfig = RECIPE_FIELDS[LOOKML];
            const { fields } = lookmlConfig;

            // LOOKML_GIT_INFO_REPO should be the first field
            expect(fields[0]).toBe(LOOKML_GIT_INFO_REPO);
        });

        it('should not contain deprecated github_info references', () => {
            const lookmlConfig = RECIPE_FIELDS[LOOKML];
            const { fields } = lookmlConfig;

            // Check that no fields have github_info in their name
            const githubInfoFields = fields.filter((field) => field.name && field.name.includes('github_info'));
            expect(githubInfoFields).toHaveLength(0);
        });

        it('should contain git_info references', () => {
            const lookmlConfig = RECIPE_FIELDS[LOOKML];
            const { fields } = lookmlConfig;

            // Check that fields have git_info in their name
            const gitInfoFields = fields.filter((field) => field.name && field.name.includes('git_info'));
            expect(gitInfoFields.length).toBeGreaterThan(0);
        });
    });

    describe('Field Consistency', () => {
        it('should have consistent field paths for git_info', () => {
            const lookmlConfig = RECIPE_FIELDS[LOOKML];
            const { fields } = lookmlConfig;

            const gitInfoFields = fields.filter((field) => field.name && field.name.includes('git_info'));

            gitInfoFields.forEach((field) => {
                expect(field.fieldPath).toMatch(/^source\.config\.git_info\./);
            });
        });

        it('should have proper field types for git_info fields', () => {
            const lookmlConfig = RECIPE_FIELDS[LOOKML];
            const { fields } = lookmlConfig;

            const gitInfoFields = fields.filter((field) => field.name && field.name.includes('git_info'));

            gitInfoFields.forEach((field) => {
                expect(field.type).toBeDefined();
                expect(['TEXT', 'SECRET']).toContain(field.type);
            });
        });
    });

    describe('Migration from github_info', () => {
        it('should not have any github_info field references', () => {
            const lookmlConfig = RECIPE_FIELDS[LOOKML];
            const { fields } = lookmlConfig;

            // Ensure no fields reference the old github_info structure
            const oldFieldPaths = fields.filter((field) => field.fieldPath && field.fieldPath.includes('github_info'));
            expect(oldFieldPaths).toHaveLength(0);
        });

        it('should have updated field names from github_info to git_info', () => {
            const lookmlConfig = RECIPE_FIELDS[LOOKML];
            const { fields } = lookmlConfig;

            const gitInfoFields = fields.filter((field) => field.name && field.name.includes('git_info'));

            expect(gitInfoFields.length).toBeGreaterThan(0);

            gitInfoFields.forEach((field) => {
                expect(field.name).toMatch(/^git_info\./);
                expect(field.name).not.toMatch(/^github_info\./);
            });
        });
    });

    describe('Field Validation', () => {
        it('should have required fields properly marked', () => {
            const lookmlConfig = RECIPE_FIELDS[LOOKML];
            const { fields } = lookmlConfig;

            const requiredFields = fields.filter((field) => field.required === true);
            expect(requiredFields.length).toBeGreaterThan(0);
        });

        it('should have validation rules for required fields', () => {
            const lookmlConfig = RECIPE_FIELDS[LOOKML];
            const { fields } = lookmlConfig;

            const fieldsWithRules = fields.filter((field) => field.rules && field.rules.length > 0);

            expect(fieldsWithRules.length).toBeGreaterThan(0);
        });
    });
});
