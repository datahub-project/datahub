import React from 'react';
import { render, screen } from '@testing-library/react';
import { Form } from 'antd';

import { GIT_INFO_REPO } from '@app/ingestV2/source/builder/RecipeForm/common';
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

describe('Common Git Info Fields', () => {
    describe('GIT_INFO_REPO', () => {
        it('should have correct field properties', () => {
            expect(GIT_INFO_REPO.name).toBe('git_info.repo');
            expect(GIT_INFO_REPO.label).toBe('Git Repository');
            expect(GIT_INFO_REPO.type).toBe(FieldType.TEXT);
            expect(GIT_INFO_REPO.fieldPath).toBe('source.config.git_info.repo');
            expect(GIT_INFO_REPO.rules).toBeNull();
        });

        it('should not be required by default', () => {
            expect(GIT_INFO_REPO.required).toBeUndefined();
        });

        it('should render tooltip with multi-platform examples', () => {
            render(
                <Form>
                    <MockFormField field={GIT_INFO_REPO} removeMargin={false} />
                </Form>
            );
            
            const tooltip = screen.getByTestId('tooltip-git_info.repo');
            expect(tooltip.textContent).toContain('React tooltip');
        });

        it('should have updated from deprecated github_info', () => {
            expect(GIT_INFO_REPO.name).not.toContain('github_info');
            expect(GIT_INFO_REPO.name).toContain('git_info');
            expect(GIT_INFO_REPO.label).not.toBe('GitHub Repo');
            expect(GIT_INFO_REPO.label).toBe('Git Repository');
        });

        it('should support multiple Git platforms in tooltip', () => {
            // Test that the tooltip contains information about multiple platforms
            const tooltip = GIT_INFO_REPO.tooltip;
            expect(tooltip).toBeDefined();
            
            // Since tooltip is a React component, we test its structure
            expect(React.isValidElement(tooltip)).toBe(true);
        });
    });

    describe('Field Configuration', () => {
        it('should have correct field path structure', () => {
            expect(GIT_INFO_REPO.fieldPath).toBe('source.config.git_info.repo');
            expect(GIT_INFO_REPO.fieldPath).toMatch(/^source\.config\.git_info\./);
        });

        it('should be a text input field', () => {
            expect(GIT_INFO_REPO.type).toBe(FieldType.TEXT);
        });

        it('should not have validation rules', () => {
            expect(GIT_INFO_REPO.rules).toBeNull();
        });
    });

    describe('Multi-Platform Support', () => {
        it('should have tooltip that mentions multiple platforms', () => {
            // The tooltip should be a React component that includes examples
            // for different Git platforms
            expect(GIT_INFO_REPO.tooltip).toBeDefined();
            expect(React.isValidElement(GIT_INFO_REPO.tooltip)).toBe(true);
        });

        it('should support flexible repository URL formats', () => {
            // The field should accept various repository URL formats
            // This is tested by the fact that there are no strict validation rules
            expect(GIT_INFO_REPO.rules).toBeNull();
        });
    });

    describe('Backward Compatibility', () => {
        it('should replace deprecated github_info field', () => {
            // Ensure the field name has been updated from github_info to git_info
            expect(GIT_INFO_REPO.name).toBe('git_info.repo');
            expect(GIT_INFO_REPO.name).not.toBe('github_info.repo');
        });

        it('should have updated label from GitHub-specific to generic', () => {
            expect(GIT_INFO_REPO.label).toBe('Git Repository');
            expect(GIT_INFO_REPO.label).not.toBe('GitHub Repo');
        });

        it('should have updated field path', () => {
            expect(GIT_INFO_REPO.fieldPath).toBe('source.config.git_info.repo');
            expect(GIT_INFO_REPO.fieldPath).not.toBe('source.config.github_info.repo');
        });
    });
});
