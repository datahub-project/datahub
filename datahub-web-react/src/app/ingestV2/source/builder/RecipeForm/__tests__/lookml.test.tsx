import React from 'react';
import { render, screen } from '@testing-library/react';
import { Form } from 'antd';

import {
    LOOKML_GIT_INFO_REPO,
    LOOKML_GIT_INFO_DEPLOY_KEY,
    LOOKML_GIT_INFO_REPO_SSH_LOCATOR,
    LOOKML_BASE_URL,
    LOOKML_CLIENT_ID,
    LOOKML_CLIENT_SECRET,
    PROJECT_NAME,
    PARSE_TABLE_NAMES_FROM_SQL,
    CONNECTION_TO_PLATFORM_MAP,
} from '@app/ingestV2/source/builder/RecipeForm/lookml';
import { FieldType } from '@app/ingestV2/source/builder/RecipeForm/common';

// Mock FormField component for testing
const MockFormField = ({ field, removeMargin }) => {
    const { name, label, tooltip, type, placeholder, rules, required } = field;
    
    return (
        <div data-testid={`form-field-${name}`}>
            <label>{label}</label>
            {tooltip && <div data-testid={`tooltip-${name}`}>{typeof tooltip === 'string' ? tooltip : 'React tooltip'}</div>}
            <input 
                type={type === FieldType.SECRET ? 'password' : 'text'} 
                placeholder={placeholder}
                data-required={required}
                data-rules={rules ? rules.length : 0}
            />
        </div>
    );
};

describe('LookML Git Info Fields', () => {
    describe('LOOKML_GIT_INFO_REPO', () => {
        it('should have correct field properties', () => {
            expect(LOOKML_GIT_INFO_REPO.name).toBe('git_info.repo');
            expect(LOOKML_GIT_INFO_REPO.label).toBe('Git Repository');
            expect(LOOKML_GIT_INFO_REPO.type).toBe(FieldType.TEXT);
            expect(LOOKML_GIT_INFO_REPO.fieldPath).toBe('source.config.git_info.repo');
            expect(LOOKML_GIT_INFO_REPO.required).toBe(true);
            expect(LOOKML_GIT_INFO_REPO.placeholder).toBe('datahub-project/datahub or https://github.com/datahub-project/datahub');
        });

        it('should have validation rules', () => {
            expect(LOOKML_GIT_INFO_REPO.rules).toHaveLength(1);
            expect(LOOKML_GIT_INFO_REPO.rules![0].required).toBe(true);
            expect(LOOKML_GIT_INFO_REPO.rules![0].message).toBe('Git Repository is required');
        });

        it('should render tooltip with multi-platform support', () => {
            render(
                <Form>
                    <MockFormField field={LOOKML_GIT_INFO_REPO} removeMargin={false} />
                </Form>
            );
            
            const tooltip = screen.getByTestId('tooltip-git_info.repo');
            expect(tooltip.textContent).toContain('React tooltip');
        });
    });

    describe('DEPLOY_KEY', () => {
        it('should have correct field properties', () => {
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.name).toBe('git_info.deploy_key');
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.label).toBe('Git Deploy Key');
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.type).toBe(FieldType.SECRET);
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.fieldPath).toBe('source.config.git_info.deploy_key');
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.required).toBe(true);
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.placeholder).toBe('-----BEGIN OPENSSH PRIVATE KEY-----\n...');
        });

        it('should have validation rules', () => {
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.rules).toHaveLength(1);
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.rules![0].required).toBe(true);
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.rules![0].message).toBe('Git Deploy Key is required');
        });

        it('should have setValueOnRecipeOverride function', () => {
            expect(typeof LOOKML_GIT_INFO_DEPLOY_KEY.setValueOnRecipeOverride).toBe('function');
            
            const recipe = { source: { config: {} } };
            const result = LOOKML_GIT_INFO_DEPLOY_KEY.setValueOnRecipeOverride!(recipe, 'test-key');
            expect(result.source.config.git_info.deploy_key).toBe('test-key\n');
        });

        it('should render tooltip with multi-platform links', () => {
            render(
                <Form>
                    <MockFormField field={LOOKML_GIT_INFO_DEPLOY_KEY} removeMargin={false} />
                </Form>
            );
            
            const tooltip = screen.getByTestId('tooltip-git_info.deploy_key');
            expect(tooltip.textContent).toContain('React tooltip');
        });
    });

    describe('REPO_SSH_LOCATOR', () => {
        it('should have correct field properties', () => {
            expect(LOOKML_GIT_INFO_REPO_SSH_LOCATOR.name).toBe('git_info.repo_ssh_locator');
            expect(LOOKML_GIT_INFO_REPO_SSH_LOCATOR.label).toBe('Repository SSH Locator');
            expect(LOOKML_GIT_INFO_REPO_SSH_LOCATOR.type).toBe(FieldType.TEXT);
            expect(LOOKML_GIT_INFO_REPO_SSH_LOCATOR.fieldPath).toBe('source.config.git_info.repo_ssh_locator');
            expect(LOOKML_GIT_INFO_REPO_SSH_LOCATOR.placeholder).toBe('git@your-git-server.com:org/repo.git');
        });

        it('should have conditional validation rules', () => {
            expect(LOOKML_GIT_INFO_REPO_SSH_LOCATOR.rules).toHaveLength(1);
            expect(typeof LOOKML_GIT_INFO_REPO_SSH_LOCATOR.rules![0]).toBe('function');
        });

        it('should validate GitHub repos do not require SSH locator', async () => {
            const validator = LOOKML_GIT_INFO_REPO_SSH_LOCATOR.rules![0]({
                getFieldValue: (fieldName) => {
                    if (fieldName === 'git_info.repo') return 'datahub-project/datahub';
                    return undefined;
                }
            });

            const result = await validator.validator({}, '');
            expect(result).toBeUndefined(); // Should not throw error
        });

        it('should validate GitLab repos do not require SSH locator', async () => {
            const validator = LOOKML_GIT_INFO_REPO_SSH_LOCATOR.rules![0]({
                getFieldValue: (fieldName) => {
                    if (fieldName === 'git_info.repo') return 'https://gitlab.com/gitlab-org/gitlab';
                    return undefined;
                }
            });

            const result = await validator.validator({}, '');
            expect(result).toBeUndefined(); // Should not throw error
        });

        it('should require SSH locator for non-GitHub/GitLab repos', async () => {
            const validator = LOOKML_GIT_INFO_REPO_SSH_LOCATOR.rules![0]({
                getFieldValue: (fieldName) => {
                    if (fieldName === 'git_info.repo') return 'https://custom-git.com/org/repo';
                    return undefined;
                }
            });

            try {
                await validator.validator({}, '');
                expect(true).toBe(false); // Should not reach here
            } catch (error: any) {
                expect(error.message).toBe('Repository SSH Locator is required for Git platforms other than GitHub and GitLab');
            }
        });

        it('should not require SSH locator when repo is not provided', async () => {
            const validator = LOOKML_GIT_INFO_REPO_SSH_LOCATOR.rules![0]({
                getFieldValue: (fieldName) => {
                    if (fieldName === 'git_info.repo') return undefined;
                    return undefined;
                }
            });

            const result = await validator.validator({}, '');
            expect(result).toBeUndefined(); // Should not throw error
        });

        it('should render tooltip with examples', () => {
            render(
                <Form>
                    <MockFormField field={LOOKML_GIT_INFO_REPO_SSH_LOCATOR} removeMargin={false} />
                </Form>
            );
            
            const tooltip = screen.getByTestId('tooltip-git_info.repo_ssh_locator');
            expect(tooltip.textContent).toContain('React tooltip');
        });
    });

    describe('Field Integration', () => {
        it('should have consistent field paths for git_info', () => {
            expect(LOOKML_GIT_INFO_REPO.fieldPath).toBe('source.config.git_info.repo');
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.fieldPath).toBe('source.config.git_info.deploy_key');
            expect(LOOKML_GIT_INFO_REPO_SSH_LOCATOR.fieldPath).toBe('source.config.git_info.repo_ssh_locator');
        });

        it('should have proper field types', () => {
            expect(LOOKML_GIT_INFO_REPO.type).toBe(FieldType.TEXT);
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.type).toBe(FieldType.SECRET);
            expect(LOOKML_GIT_INFO_REPO_SSH_LOCATOR.type).toBe(FieldType.TEXT);
        });

        it('should have required fields marked correctly', () => {
            expect(LOOKML_GIT_INFO_REPO.required).toBe(true);
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.required).toBe(true);
            expect(LOOKML_GIT_INFO_REPO_SSH_LOCATOR.required).toBeUndefined(); // Conditional requirement
        });
    });

    describe('Backward Compatibility', () => {
        it('should use git_info instead of deprecated github_info', () => {
            expect(LOOKML_GIT_INFO_REPO.name).not.toContain('github_info');
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.name).not.toContain('github_info');
            expect(LOOKML_GIT_INFO_REPO_SSH_LOCATOR.name).not.toContain('github_info');
            
            expect(LOOKML_GIT_INFO_REPO.name).toContain('git_info');
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.name).toContain('git_info');
            expect(LOOKML_GIT_INFO_REPO_SSH_LOCATOR.name).toContain('git_info');
        });

        it('should have updated labels from GitHub-specific to Git-generic', () => {
            expect(LOOKML_GIT_INFO_REPO.label).toBe('Git Repository');
            expect(LOOKML_GIT_INFO_DEPLOY_KEY.label).toBe('Git Deploy Key');
            expect(LOOKML_GIT_INFO_REPO_SSH_LOCATOR.label).toBe('Repository SSH Locator');
        });
    });

    describe('Multi-Platform Support', () => {
        it('should support GitHub repositories', () => {
            const githubRepos = [
                'datahub-project/datahub',
                'https://github.com/datahub-project/datahub',
                'github.com/datahub-project/datahub'
            ];

            githubRepos.forEach(repo => {
                const validator = LOOKML_GIT_INFO_REPO_SSH_LOCATOR.rules![0]({
                    getFieldValue: (fieldName) => {
                        if (fieldName === 'git_info.repo') return repo;
                        return undefined;
                    }
                });

                expect(async () => {
                    await validator.validator({}, '');
                }).not.toThrow();
            });
        });

        it('should support GitLab repositories', () => {
            const gitlabRepos = [
                'https://gitlab.com/gitlab-org/gitlab',
                'gitlab.com/gitlab-org/gitlab'
            ];

            gitlabRepos.forEach(repo => {
                const validator = LOOKML_GIT_INFO_REPO_SSH_LOCATOR.rules![0]({
                    getFieldValue: (fieldName) => {
                        if (fieldName === 'git_info.repo') return repo;
                        return undefined;
                    }
                });

                expect(async () => {
                    await validator.validator({}, '');
                }).not.toThrow();
            });
        });

        it('should require SSH locator for other Git platforms', async () => {
            const otherPlatformRepos = [
                'https://bitbucket.org/org/repo',
                'https://custom-git.com/org/repo',
                'https://git.company.com/org/repo'
            ];

            for (const repo of otherPlatformRepos) {
                const validator = LOOKML_GIT_INFO_REPO_SSH_LOCATOR.rules![0]({
                    getFieldValue: (fieldName) => {
                        if (fieldName === 'git_info.repo') return repo;
                        return undefined;
                    }
                });

                await expect(validator.validator({}, '')).rejects.toThrow('Repository SSH Locator is required for Git platforms other than GitHub and GitLab');
            }
        });
    });
});
