import { describe, expect, it } from 'vitest';

import {
    CONFLUENCE_API_TOKEN,
    CONFLUENCE_DEPLOYMENT_TYPE,
    CONFLUENCE_PAGE_ALLOW,
    CONFLUENCE_PAGE_DENY,
    CONFLUENCE_PERSONAL_ACCESS_TOKEN,
    CONFLUENCE_SPACE_ALLOW,
    CONFLUENCE_SPACE_DENY,
    CONFLUENCE_URL,
    CONFLUENCE_USERNAME,
} from '@app/ingestV2/source/builder/RecipeForm/confluence';
import { RECIPE_FIELDS, RecipeSections } from '@app/ingestV2/source/builder/RecipeForm/constants';
import { CONFLUENCE } from '@app/ingestV2/source/builder/constants';

describe('Confluence deployment type helpers', () => {
    describe('getDeploymentTypeFromRecipe', () => {
        it('should return cloud when cloud is true', () => {
            const recipe = {
                source: {
                    config: {
                        cloud: true,
                    },
                },
            };
            const result = CONFLUENCE_DEPLOYMENT_TYPE.getValueFromRecipeOverride?.(recipe);

            expect(result).toBe('cloud');
        });

        it('should return cloud when username is present', () => {
            const recipe = {
                source: {
                    config: {
                        username: 'user@example.com',
                    },
                },
            };
            const result = CONFLUENCE_DEPLOYMENT_TYPE.getValueFromRecipeOverride?.(recipe);

            expect(result).toBe('cloud');
        });

        it('should return cloud when api_token is present', () => {
            const recipe = {
                source: {
                    config: {
                        api_token: 'token123',
                    },
                },
            };
            const result = CONFLUENCE_DEPLOYMENT_TYPE.getValueFromRecipeOverride?.(recipe);

            expect(result).toBe('cloud');
        });

        it('should return datacenter when cloud is false', () => {
            const recipe = {
                source: {
                    config: {
                        cloud: false,
                    },
                },
            };
            const result = CONFLUENCE_DEPLOYMENT_TYPE.getValueFromRecipeOverride?.(recipe);

            expect(result).toBe('datacenter');
        });

        it('should return datacenter when personal_access_token is present', () => {
            const recipe = {
                source: {
                    config: {
                        personal_access_token: 'pat-token',
                    },
                },
            };
            const result = CONFLUENCE_DEPLOYMENT_TYPE.getValueFromRecipeOverride?.(recipe);

            expect(result).toBe('datacenter');
        });

        it('should default to cloud when no credentials present', () => {
            const recipe = {
                source: {
                    config: {},
                },
            };
            const result = CONFLUENCE_DEPLOYMENT_TYPE.getValueFromRecipeOverride?.(recipe);

            expect(result).toBe('cloud');
        });

        it('should prioritize cloud credentials when both are present', () => {
            const recipe = {
                source: {
                    config: {
                        username: 'user@example.com',
                        api_token: 'token123',
                        personal_access_token: 'pat-token',
                    },
                },
            };
            const result = CONFLUENCE_DEPLOYMENT_TYPE.getValueFromRecipeOverride?.(recipe);

            expect(result).toBe('cloud');
        });
    });

    describe('setDeploymentTypeOnRecipe', () => {
        it('should set cloud to true and remove PAT when deployment type is cloud', () => {
            const recipe = {
                source: {
                    config: {
                        personal_access_token: 'pat-token',
                    },
                },
            };
            const result = CONFLUENCE_DEPLOYMENT_TYPE.setValueOnRecipeOverride?.(recipe, 'cloud');

            expect(result.source.config.cloud).toBe(true);
            expect(result.source.config.personal_access_token).toBeUndefined();
        });

        it('should set cloud to false and remove username/api_token when deployment type is datacenter', () => {
            const recipe = {
                source: {
                    config: {
                        username: 'user@example.com',
                        api_token: 'token123',
                    },
                },
            };
            const result = CONFLUENCE_DEPLOYMENT_TYPE.setValueOnRecipeOverride?.(recipe, 'datacenter');

            expect(result.source.config.cloud).toBe(false);
            expect(result.source.config.username).toBeUndefined();
            expect(result.source.config.api_token).toBeUndefined();
        });

        it('should preserve other config fields when setting deployment type', () => {
            const recipe = {
                source: {
                    config: {
                        url: 'https://example.atlassian.net/wiki',
                        spaces: {
                            allow: ['TEAM', 'ENG'],
                        },
                    },
                },
            };
            const result = CONFLUENCE_DEPLOYMENT_TYPE.setValueOnRecipeOverride?.(recipe, 'cloud');

            expect(result.source.config.url).toBe('https://example.atlassian.net/wiki');
            expect(result.source.config.spaces.allow).toEqual(['TEAM', 'ENG']);
            expect(result.source.config.cloud).toBe(true);
        });
    });
});

describe('Confluence credential field visibility', () => {
    describe('CONFLUENCE_USERNAME dynamic behavior', () => {
        it('should be required when deployment type is cloud', () => {
            const values = { deployment_type: 'cloud' };
            const isRequired = CONFLUENCE_USERNAME.dynamicRequired?.(values);

            expect(isRequired).toBe(true);
        });

        it('should not be required when deployment type is datacenter', () => {
            const values = { deployment_type: 'datacenter' };
            const isRequired = CONFLUENCE_USERNAME.dynamicRequired?.(values);

            expect(isRequired).toBe(false);
        });

        it('should be hidden when deployment type is datacenter', () => {
            const values = { deployment_type: 'datacenter' };
            const isHidden = CONFLUENCE_USERNAME.dynamicHidden?.(values);

            expect(isHidden).toBe(true);
        });

        it('should not be hidden when deployment type is cloud', () => {
            const values = { deployment_type: 'cloud' };
            const isHidden = CONFLUENCE_USERNAME.dynamicHidden?.(values);

            expect(isHidden).toBe(false);
        });
    });

    describe('CONFLUENCE_API_TOKEN dynamic behavior', () => {
        it('should be required when deployment type is cloud', () => {
            const values = { deployment_type: 'cloud' };
            const isRequired = CONFLUENCE_API_TOKEN.dynamicRequired?.(values);

            expect(isRequired).toBe(true);
        });

        it('should not be required when deployment type is datacenter', () => {
            const values = { deployment_type: 'datacenter' };
            const isRequired = CONFLUENCE_API_TOKEN.dynamicRequired?.(values);

            expect(isRequired).toBe(false);
        });

        it('should be hidden when deployment type is datacenter', () => {
            const values = { deployment_type: 'datacenter' };
            const isHidden = CONFLUENCE_API_TOKEN.dynamicHidden?.(values);

            expect(isHidden).toBe(true);
        });

        it('should not be hidden when deployment type is cloud', () => {
            const values = { deployment_type: 'cloud' };
            const isHidden = CONFLUENCE_API_TOKEN.dynamicHidden?.(values);

            expect(isHidden).toBe(false);
        });
    });

    describe('CONFLUENCE_PERSONAL_ACCESS_TOKEN dynamic behavior', () => {
        it('should be required when deployment type is datacenter', () => {
            const values = { deployment_type: 'datacenter' };
            const isRequired = CONFLUENCE_PERSONAL_ACCESS_TOKEN.dynamicRequired?.(values);

            expect(isRequired).toBe(true);
        });

        it('should not be required when deployment type is cloud', () => {
            const values = { deployment_type: 'cloud' };
            const isRequired = CONFLUENCE_PERSONAL_ACCESS_TOKEN.dynamicRequired?.(values);

            expect(isRequired).toBe(false);
        });

        it('should be hidden when deployment type is cloud', () => {
            const values = { deployment_type: 'cloud' };
            const isHidden = CONFLUENCE_PERSONAL_ACCESS_TOKEN.dynamicHidden?.(values);

            expect(isHidden).toBe(true);
        });

        it('should not be hidden when deployment type is datacenter', () => {
            const values = { deployment_type: 'datacenter' };
            const isHidden = CONFLUENCE_PERSONAL_ACCESS_TOKEN.dynamicHidden?.(values);

            expect(isHidden).toBe(false);
        });
    });

    describe('Field behavior with undefined deployment type', () => {
        it('should hide all credential fields when deployment_type is undefined', () => {
            const values = {};

            // All fields should be hidden when deployment_type is undefined
            expect(CONFLUENCE_USERNAME.dynamicHidden?.(values)).toBe(true);
            expect(CONFLUENCE_API_TOKEN.dynamicHidden?.(values)).toBe(true);
            expect(CONFLUENCE_PERSONAL_ACCESS_TOKEN.dynamicHidden?.(values)).toBe(true);
        });
    });
});

describe('Confluence field configuration', () => {
    it('should have deployment type field defined', () => {
        expect(CONFLUENCE_DEPLOYMENT_TYPE).toBeDefined();
        expect(CONFLUENCE_DEPLOYMENT_TYPE.name).toBe('deployment_type');
        expect(CONFLUENCE_DEPLOYMENT_TYPE.label).toBe('Deployment Type');
    });

    it('should have deployment type field with correct options', () => {
        expect(CONFLUENCE_DEPLOYMENT_TYPE.options).toHaveLength(2);
        expect(CONFLUENCE_DEPLOYMENT_TYPE.options?.[0].value).toBe('cloud');
        expect(CONFLUENCE_DEPLOYMENT_TYPE.options?.[1].value).toBe('datacenter');
    });

    it('should have URL field defined', () => {
        expect(CONFLUENCE_URL).toBeDefined();
        expect(CONFLUENCE_URL.name).toBe('url');
        expect(CONFLUENCE_URL.required).toBe(true);
    });

    it('should have space allow/deny fields defined with correct sections', () => {
        expect(CONFLUENCE_SPACE_ALLOW).toBeDefined();
        expect(CONFLUENCE_SPACE_ALLOW.name).toBe('space_allow');
        expect(CONFLUENCE_SPACE_ALLOW.section).toBe('Spaces');
        expect(CONFLUENCE_SPACE_ALLOW.label).toBe('Include');

        expect(CONFLUENCE_SPACE_DENY).toBeDefined();
        expect(CONFLUENCE_SPACE_DENY.name).toBe('space_deny');
        expect(CONFLUENCE_SPACE_DENY.section).toBe('Spaces');
        expect(CONFLUENCE_SPACE_DENY.label).toBe('Exclude');
    });

    it('should have page allow/deny fields defined with correct sections', () => {
        expect(CONFLUENCE_PAGE_ALLOW).toBeDefined();
        expect(CONFLUENCE_PAGE_ALLOW.name).toBe('page_allow');
        expect(CONFLUENCE_PAGE_ALLOW.section).toBe('Pages');
        expect(CONFLUENCE_PAGE_ALLOW.label).toBe('Include');

        expect(CONFLUENCE_PAGE_DENY).toBeDefined();
        expect(CONFLUENCE_PAGE_DENY.name).toBe('page_deny');
        expect(CONFLUENCE_PAGE_DENY.section).toBe('Pages');
        expect(CONFLUENCE_PAGE_DENY.label).toBe('Exclude');
    });

    it('should have filter section auto-expanded by default', () => {
        expect(RECIPE_FIELDS[CONFLUENCE].defaultOpenSections).toContain(RecipeSections.Filter);
    });

    it('should have filter section tooltip', () => {
        expect(RECIPE_FIELDS[CONFLUENCE].filterSectionTooltip).toBeDefined();
    });
});

describe('Confluence RECIPE_FIELDS integration', () => {
    it('should have Confluence registered in RECIPE_FIELDS', () => {
        expect(RECIPE_FIELDS[CONFLUENCE]).toBeDefined();
    });

    it('should include CONFLUENCE_DEPLOYMENT_TYPE in fields array', () => {
        const confluenceFields = RECIPE_FIELDS[CONFLUENCE].fields;

        expect(confluenceFields).toContain(CONFLUENCE_DEPLOYMENT_TYPE);
    });

    it('should include all required credential fields in fields array', () => {
        const confluenceFields = RECIPE_FIELDS[CONFLUENCE].fields;

        expect(confluenceFields).toContain(CONFLUENCE_URL);
        expect(confluenceFields).toContain(CONFLUENCE_USERNAME);
        expect(confluenceFields).toContain(CONFLUENCE_API_TOKEN);
        expect(confluenceFields).toContain(CONFLUENCE_PERSONAL_ACCESS_TOKEN);
    });

    it('should include filtering fields in filterFields array', () => {
        const confluenceFilterFields = RECIPE_FIELDS[CONFLUENCE].filterFields;

        expect(confluenceFilterFields).toContain(CONFLUENCE_SPACE_ALLOW);
        expect(confluenceFilterFields).toContain(CONFLUENCE_SPACE_DENY);
        expect(confluenceFilterFields).toContain(CONFLUENCE_PAGE_ALLOW);
        expect(confluenceFilterFields).toContain(CONFLUENCE_PAGE_DENY);
    });

    it('should have deployment type as first field for better UX', () => {
        const confluenceFields = RECIPE_FIELDS[CONFLUENCE].fields;

        expect(confluenceFields[0]).toBe(CONFLUENCE_DEPLOYMENT_TYPE);
    });

    it('should not require hasDynamicFields flag (dynamic behavior works without it)', () => {
        // Confluence uses dynamicRequired/dynamicHidden which work without hasDynamicFields flag
        expect(RECIPE_FIELDS[CONFLUENCE].hasDynamicFields).toBeUndefined();
    });

    it('should have filter fields and empty advanced fields', () => {
        expect(RECIPE_FIELDS[CONFLUENCE].filterFields).toHaveLength(4);
        expect(RECIPE_FIELDS[CONFLUENCE].advancedFields).toEqual([]);
    });
});
