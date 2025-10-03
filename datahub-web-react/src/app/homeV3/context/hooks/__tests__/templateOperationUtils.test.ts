import { message } from 'antd';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
    TemplateUpdateContext,
    getTemplateToUpdate,
    handleValidationError,
    persistTemplateChanges,
    updateTemplateStateOptimistically,
    validateTemplateAvailability,
} from '@app/homeV3/context/hooks/utils/templateOperationUtils';

import { PageTemplateFragment } from '@graphql/template.generated';
import { EntityType, PageTemplateScope, PageTemplateSurfaceType } from '@types';

// Mock antd message
vi.mock('antd', () => ({
    message: {
        error: vi.fn(() => ({ key: 'test-message' })),
    },
}));

// Mock template data
const mockPersonalTemplate: PageTemplateFragment = {
    urn: 'urn:li:pageTemplate:personal',
    type: EntityType.DatahubPageTemplate,
    properties: {
        rows: [
            {
                modules: [
                    {
                        urn: 'urn:li:pageModule:1',
                        type: EntityType.DatahubPageModule,
                        properties: {
                            name: 'Personal Module 1',
                            type: 'LINK' as any,
                            visibility: { scope: 'PERSONAL' as any },
                            params: {},
                        },
                    },
                ],
            },
        ],
        surface: { surfaceType: PageTemplateSurfaceType.HomePage },
        visibility: { scope: PageTemplateScope.Personal },
    },
};

const mockGlobalTemplate: PageTemplateFragment = {
    urn: 'urn:li:pageTemplate:global',
    type: EntityType.DatahubPageTemplate,
    properties: {
        rows: [
            {
                modules: [
                    {
                        urn: 'urn:li:pageModule:2',
                        type: EntityType.DatahubPageModule,
                        properties: {
                            name: 'Global Module 1',
                            type: 'LINK' as any,
                            visibility: { scope: 'GLOBAL' as any },
                            params: {},
                        },
                    },
                ],
            },
        ],
        surface: { surfaceType: PageTemplateSurfaceType.HomePage },
        visibility: { scope: PageTemplateScope.Global },
    },
};

// Mock functions
const mockSetPersonalTemplate = vi.fn();
const mockSetGlobalTemplate = vi.fn();
const mockUpsertTemplate = vi.fn();

describe('Template Operation Utils', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('getTemplateToUpdate', () => {
        it('should return personal template when not editing global and personal template exists', () => {
            const context: TemplateUpdateContext = {
                isEditingGlobalTemplate: false,
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                upsertTemplate: mockUpsertTemplate,
            };

            const result = getTemplateToUpdate(context);

            expect(result.template).toBe(mockPersonalTemplate);
            expect(result.isPersonal).toBe(true);
        });

        it('should return global template when not editing global and personal template is null', () => {
            const context: TemplateUpdateContext = {
                isEditingGlobalTemplate: false,
                personalTemplate: null,
                globalTemplate: mockGlobalTemplate,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                upsertTemplate: mockUpsertTemplate,
            };

            const result = getTemplateToUpdate(context);

            expect(result.template).toBe(mockGlobalTemplate);
            expect(result.isPersonal).toBe(true);
        });

        it('should return global template when editing global', () => {
            const context: TemplateUpdateContext = {
                isEditingGlobalTemplate: true,
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                upsertTemplate: mockUpsertTemplate,
            };

            const result = getTemplateToUpdate(context);

            expect(result.template).toBe(mockGlobalTemplate);
            expect(result.isPersonal).toBe(false);
        });

        it('should handle null global template when editing global', () => {
            const context: TemplateUpdateContext = {
                isEditingGlobalTemplate: true,
                personalTemplate: mockPersonalTemplate,
                globalTemplate: null,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                upsertTemplate: mockUpsertTemplate,
            };

            const result = getTemplateToUpdate(context);

            expect(result.template).toBe(null);
            expect(result.isPersonal).toBe(false);
        });
    });

    describe('updateTemplateStateOptimistically', () => {
        it('should update personal template when isPersonal is true', () => {
            const context: TemplateUpdateContext = {
                isEditingGlobalTemplate: false,
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                upsertTemplate: mockUpsertTemplate,
            };

            const updatedTemplate = { ...mockPersonalTemplate, urn: 'urn:li:pageTemplate:updated' };

            updateTemplateStateOptimistically(context, updatedTemplate, true);

            expect(mockSetPersonalTemplate).toHaveBeenCalledWith(updatedTemplate);
            expect(mockSetGlobalTemplate).not.toHaveBeenCalled();
        });

        it('should update global template when isPersonal is false', () => {
            const context: TemplateUpdateContext = {
                isEditingGlobalTemplate: true,
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                upsertTemplate: mockUpsertTemplate,
            };

            const updatedTemplate = { ...mockGlobalTemplate, urn: 'urn:li:pageTemplate:updated' };

            updateTemplateStateOptimistically(context, updatedTemplate, false);

            expect(mockSetGlobalTemplate).toHaveBeenCalledWith(updatedTemplate);
            expect(mockSetPersonalTemplate).not.toHaveBeenCalled();
        });

        it('should handle null template', () => {
            const context: TemplateUpdateContext = {
                isEditingGlobalTemplate: false,
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                upsertTemplate: mockUpsertTemplate,
            };

            updateTemplateStateOptimistically(context, null, true);

            expect(mockSetPersonalTemplate).toHaveBeenCalledWith(null);
            expect(mockSetGlobalTemplate).not.toHaveBeenCalled();
        });
    });

    describe('persistTemplateChanges', () => {
        it('should successfully persist template changes', async () => {
            const context: TemplateUpdateContext = {
                isEditingGlobalTemplate: false,
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                upsertTemplate: mockUpsertTemplate,
            };

            const updatedTemplate = { ...mockPersonalTemplate, urn: 'urn:li:pageTemplate:updated' };
            mockUpsertTemplate.mockResolvedValue({});

            await persistTemplateChanges(context, updatedTemplate, true, 'test operation');

            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, true, mockPersonalTemplate);
            expect(mockSetPersonalTemplate).not.toHaveBeenCalledWith(mockPersonalTemplate); // No revert
        });

        it('should revert personal template on error', async () => {
            const context: TemplateUpdateContext = {
                isEditingGlobalTemplate: false,
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                upsertTemplate: mockUpsertTemplate,
            };

            const updatedTemplate = { ...mockPersonalTemplate, urn: 'urn:li:pageTemplate:updated' };
            const error = new Error('Upsert failed');
            mockUpsertTemplate.mockRejectedValue(error);

            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

            await persistTemplateChanges(context, updatedTemplate, true, 'test operation');

            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, true, mockPersonalTemplate);
            expect(mockSetPersonalTemplate).toHaveBeenCalledWith(mockPersonalTemplate); // Revert call
            expect(consoleSpy).toHaveBeenCalledWith('Failed to test operation:', error);
            expect(message.error).toHaveBeenCalledWith('Failed to test operation');

            consoleSpy.mockRestore();
        });

        it('should revert global template on error', async () => {
            const context: TemplateUpdateContext = {
                isEditingGlobalTemplate: true,
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                upsertTemplate: mockUpsertTemplate,
            };

            const updatedTemplate = { ...mockGlobalTemplate, urn: 'urn:li:pageTemplate:updated' };
            const error = new Error('Upsert failed');
            mockUpsertTemplate.mockRejectedValue(error);

            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

            await persistTemplateChanges(context, updatedTemplate, false, 'test operation');

            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, false, mockPersonalTemplate);
            expect(mockSetGlobalTemplate).toHaveBeenCalledWith(mockGlobalTemplate); // Revert call
            expect(consoleSpy).toHaveBeenCalledWith('Failed to test operation:', error);
            expect(message.error).toHaveBeenCalledWith('Failed to test operation');

            consoleSpy.mockRestore();
        });

        it('should handle null template gracefully', async () => {
            const context: TemplateUpdateContext = {
                isEditingGlobalTemplate: false,
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                upsertTemplate: mockUpsertTemplate,
            };

            mockUpsertTemplate.mockResolvedValue({});

            await persistTemplateChanges(context, null, true, 'test operation');

            expect(mockUpsertTemplate).toHaveBeenCalledWith(null, true, mockPersonalTemplate);
        });
    });

    describe('handleValidationError', () => {
        it('should return false when no validation error', () => {
            vi.spyOn(console, 'error').mockImplementation(() => {});
            const result = handleValidationError(null, 'test operation');

            expect(result).toBe(false);
            expect(console.error).not.toHaveBeenCalled();
            expect(message.error).not.toHaveBeenCalled();
        });

        it('should return true and log error when validation error exists', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const validationError = 'Test validation error';

            const result = handleValidationError(validationError, 'test operation');

            expect(result).toBe(true);
            expect(consoleSpy).toHaveBeenCalledWith('Invalid test operation input:', validationError);
            expect(message.error).toHaveBeenCalledWith(validationError);

            consoleSpy.mockRestore();
        });
    });

    describe('validateTemplateAvailability', () => {
        it('should return true when template is available', () => {
            vi.spyOn(console, 'error').mockImplementation(() => {});
            const result = validateTemplateAvailability(mockPersonalTemplate);

            expect(result).toBe(true);
            expect(console.error).not.toHaveBeenCalled();
            expect(message.error).not.toHaveBeenCalled();
        });

        it('should return false and log error when template is null', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

            const result = validateTemplateAvailability(null);

            expect(result).toBe(false);
            expect(consoleSpy).toHaveBeenCalledWith('No template provided to update');
            expect(message.error).toHaveBeenCalledWith('No template available to update');

            consoleSpy.mockRestore();
        });

        it('should return false and log error when template is undefined', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

            const result = validateTemplateAvailability(undefined as any);

            expect(result).toBe(false);
            expect(consoleSpy).toHaveBeenCalledWith('No template provided to update');
            expect(message.error).toHaveBeenCalledWith('No template available to update');

            consoleSpy.mockRestore();
        });
    });
});
