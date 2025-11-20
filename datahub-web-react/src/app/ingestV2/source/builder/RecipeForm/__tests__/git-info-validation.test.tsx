import { LOOKML_GIT_INFO_REPO_SSH_LOCATOR } from '@app/ingestV2/source/builder/RecipeForm/lookml';

describe('Git Info Validation Logic', () => {
    const createValidator = (repoValue: string | undefined | null) => {
        return LOOKML_GIT_INFO_REPO_SSH_LOCATOR.rules![0]({
            getFieldValue: (fieldName) => {
                if (fieldName === 'git_info.repo') return repoValue;
                return undefined;
            },
        });
    };

    describe('REPO_SSH_LOCATOR Validation', () => {
        describe('GitHub Repository Detection', () => {
            it('should not require SSH locator for GitHub short format', async () => {
                const validator = createValidator('datahub-project/datahub');
                await expect(validator.validator({}, '')).resolves.toBeUndefined();
            });

            it('should not require SSH locator for GitHub full URL', async () => {
                const validator = createValidator('https://github.com/datahub-project/datahub');
                await expect(validator.validator({}, '')).resolves.toBeUndefined();
            });

            it('should not require SSH locator for GitHub URL without protocol', async () => {
                const validator = createValidator('github.com/datahub-project/datahub');
                await expect(validator.validator({}, '')).resolves.toBeUndefined();
            });

            it('should not require SSH locator for GitHub URL with trailing slash', async () => {
                const validator = createValidator('https://github.com/datahub-project/datahub/');
                await expect(validator.validator({}, '')).resolves.toBeUndefined();
            });
        });

        describe('GitLab Repository Detection', () => {
            it('should not require SSH locator for GitLab full URL', async () => {
                const validator = createValidator('https://gitlab.com/gitlab-org/gitlab');
                await expect(validator.validator({}, '')).resolves.toBeUndefined();
            });

            it('should not require SSH locator for GitLab URL without protocol', async () => {
                const validator = createValidator('gitlab.com/gitlab-org/gitlab');
                await expect(validator.validator({}, '')).resolves.toBeUndefined();
            });

            it('should not require SSH locator for GitLab URL with trailing slash', async () => {
                const validator = createValidator('https://gitlab.com/gitlab-org/gitlab/');
                await expect(validator.validator({}, '')).resolves.toBeUndefined();
            });
        });

        describe('Other Git Platforms', () => {
            it('should require SSH locator for Bitbucket', async () => {
                const validator = createValidator('https://bitbucket.org/org/repo');
                await expect(validator.validator({}, '')).rejects.toThrow(
                    'Repository SSH Locator is required for Git platforms other than GitHub and GitLab',
                );
            });

            it('should require SSH locator for custom Git server', async () => {
                const validator = createValidator('https://custom-git.com/org/repo');
                await expect(validator.validator({}, '')).rejects.toThrow(
                    'Repository SSH Locator is required for Git platforms other than GitHub and GitLab',
                );
            });

            it('should require SSH locator for SSH URL format', async () => {
                const validator = createValidator('git@custom-server.com:org/repo');
                await expect(validator.validator({}, '')).rejects.toThrow(
                    'Repository SSH Locator is required for Git platforms other than GitHub and GitLab',
                );
            });

            it('should not require SSH locator when SSH locator is provided', async () => {
                const validator = createValidator('https://custom-git.com/org/repo');
                await expect(validator.validator({}, 'git@custom-git.com:org/repo.git')).resolves.toBeUndefined();
            });
        });

        describe('Edge Cases', () => {
            it('should not require SSH locator when repo is undefined', async () => {
                const validator = createValidator(undefined);
                await expect(validator.validator({}, '')).resolves.toBeUndefined();
            });

            it('should not require SSH locator when repo is null', async () => {
                const validator = createValidator(null);
                await expect(validator.validator({}, '')).resolves.toBeUndefined();
            });

            it('should not require SSH locator when repo is empty string', async () => {
                const validator = createValidator('');
                await expect(validator.validator({}, '')).resolves.toBeUndefined();
            });

            it('should handle case-insensitive GitHub detection', async () => {
                const validator = createValidator('https://GITHUB.COM/datahub-project/datahub');
                await expect(validator.validator({}, '')).resolves.toBeUndefined();
            });

            it('should handle case-insensitive GitLab detection', async () => {
                const validator = createValidator('https://GITLAB.COM/gitlab-org/gitlab');
                await expect(validator.validator({}, '')).resolves.toBeUndefined();
            });
        });

        describe('Complex Repository URLs', () => {
            it('should handle GitHub URLs with additional path segments', async () => {
                const validator = createValidator('https://github.com/datahub-project/datahub/tree/main/src');
                await expect(validator.validator({}, '')).resolves.toBeUndefined();
            });

            it('should handle GitLab URLs with additional path segments', async () => {
                const validator = createValidator('https://gitlab.com/gitlab-org/gitlab/-/tree/main/src');
                await expect(validator.validator({}, '')).resolves.toBeUndefined();
            });

            it('should require SSH locator for non-GitHub/GitLab URLs with similar patterns', async () => {
                const validator = createValidator('https://github-enterprise.company.com/org/repo');
                await expect(validator.validator({}, '')).rejects.toThrow(
                    'Repository SSH Locator is required for Git platforms other than GitHub and GitLab',
                );
            });
        });
    });

    describe('Validation Error Messages', () => {
        it('should provide clear error message for missing SSH locator', async () => {
            const validator = createValidator('https://custom-git.com/org/repo');
            try {
                await validator.validator({}, '');
            } catch (error: any) {
                expect(error.message).toBe(
                    'Repository SSH Locator is required for Git platforms other than GitHub and GitLab',
                );
            }
        });
    });
});
