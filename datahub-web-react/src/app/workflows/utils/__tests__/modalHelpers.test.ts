import { describe, expect, it, vi } from 'vitest';

import {
    WorkflowRequestFormModalMode,
    createFormValuesForVisibility,
    getEntitySelectorTitle,
    getModalConfig,
    getPlaceholderText,
    isEntitySelectionRequired,
    shouldShowAccessFields,
} from '@app/workflows/utils/modalHelpers';

import { ActionWorkflowCategory, EntityType } from '@types';

// Mock EntityRegistry
const mockEntityRegistry = {
    getEntityName: vi.fn(),
};

describe('modalHelpers', () => {
    describe('getEntitySelectorTitle', () => {
        beforeEach(() => {
            vi.clearAllMocks();
        });

        it('should return specific entity name when workflow has single entity type', () => {
            const workflow = {
                trigger: {
                    form: {
                        entityTypes: [EntityType.Dataset],
                    },
                },
            } as any;

            mockEntityRegistry.getEntityName.mockReturnValue('Dataset');

            const result = getEntitySelectorTitle(workflow, mockEntityRegistry as any);

            expect(mockEntityRegistry.getEntityName).toHaveBeenCalledWith(EntityType.Dataset);
            expect(result).toBe('Dataset');
        });

        it('should return "Entity" when getEntityName returns null', () => {
            const workflow = {
                trigger: {
                    form: {
                        entityTypes: [EntityType.Dataset],
                    },
                },
            } as any;

            mockEntityRegistry.getEntityName.mockReturnValue(null);

            const result = getEntitySelectorTitle(workflow, mockEntityRegistry as any);

            expect(result).toBe('Entity');
        });

        it('should return "Asset" when workflow has multiple entity types', () => {
            const workflow = {
                trigger: {
                    form: {
                        entityTypes: [EntityType.Dataset, EntityType.Dashboard],
                    },
                },
            } as any;

            const result = getEntitySelectorTitle(workflow, mockEntityRegistry as any);

            expect(result).toBe('Asset');
            expect(mockEntityRegistry.getEntityName).not.toHaveBeenCalled();
        });

        it('should return "Asset" when workflow has no entity types', () => {
            const workflow = {
                trigger: {
                    form: {
                        entityTypes: [],
                    },
                },
            } as any;

            const result = getEntitySelectorTitle(workflow, mockEntityRegistry as any);

            expect(result).toBe('Asset');
        });

        it('should return "Asset" when entityTypes is undefined', () => {
            const workflow = {} as any;

            const result = getEntitySelectorTitle(workflow, mockEntityRegistry as any);

            expect(result).toBe('Asset');
        });
    });

    describe('createFormValuesForVisibility', () => {
        it('should transform form state into values format', () => {
            const formState = {
                field1: { values: ['value1', 'value2'] },
                field2: { values: [123] },
                field3: { values: [] },
            };

            const result = createFormValuesForVisibility(formState);

            expect(result).toEqual({
                field1: ['value1', 'value2'],
                field2: [123],
                field3: [],
            });
        });

        it('should handle missing values property', () => {
            const formState = {
                field1: { values: ['value1'] },
                field2: {} as any, // Missing values property
                field3: { values: undefined } as any,
            };

            const result = createFormValuesForVisibility(formState);

            expect(result).toEqual({
                field1: ['value1'],
                field2: [],
                field3: [],
            });
        });

        it('should handle empty form state', () => {
            const result = createFormValuesForVisibility({});

            expect(result).toEqual({});
        });
    });

    describe('getModalConfig', () => {
        const mockWorkflow = {
            name: 'Test Workflow',
        } as any;

        it('should return review config for REVIEW mode', () => {
            const result = getModalConfig(WorkflowRequestFormModalMode.REVIEW, mockWorkflow);

            expect(result).toEqual({
                title: 'Review Test Workflow Request',
                submitButtonText: 'Submit',
                cancelButtonText: 'Close',
            });
        });

        it('should return create config for CREATE mode', () => {
            const result = getModalConfig(WorkflowRequestFormModalMode.CREATE, mockWorkflow);

            expect(result).toEqual({
                title: 'Create Test Workflow Request',
                submitButtonText: 'Submit',
                cancelButtonText: 'Cancel',
            });
        });

        it('should return create config for EDIT mode (defaults to create)', () => {
            const result = getModalConfig(WorkflowRequestFormModalMode.EDIT, mockWorkflow);

            expect(result).toEqual({
                title: 'Create Test Workflow Request',
                submitButtonText: 'Submit',
                cancelButtonText: 'Cancel',
            });
        });
    });

    describe('shouldShowAccessFields', () => {
        it('should return true for ACCESS workflow type', () => {
            const workflow = {
                category: ActionWorkflowCategory.Access,
            } as any;

            const result = shouldShowAccessFields(workflow);

            expect(result).toBe(true);
        });

        it('should return false for non-ACCESS workflow types', () => {
            const workflow = {
                category: 'OTHER_TYPE' as any,
            } as any;

            const result = shouldShowAccessFields(workflow);

            expect(result).toBe(false);
        });
    });

    describe('getPlaceholderText', () => {
        it('should return "No value provided" for REVIEW mode', () => {
            const result = getPlaceholderText(WorkflowRequestFormModalMode.REVIEW, 'Default text');

            expect(result).toBe('No value provided');
        });

        it('should return default text for CREATE mode', () => {
            const result = getPlaceholderText(WorkflowRequestFormModalMode.CREATE, 'Default text');

            expect(result).toBe('Default text');
        });

        it('should return default text for EDIT mode', () => {
            const result = getPlaceholderText(WorkflowRequestFormModalMode.EDIT, 'Default text');

            expect(result).toBe('Default text');
        });
    });

    describe('isEntitySelectionRequired', () => {
        it('should return true when workflow requires entities and none selected', () => {
            const workflow = {
                trigger: {
                    form: {
                        entityTypes: [EntityType.Dataset],
                    },
                },
            } as any;

            const result = isEntitySelectionRequired(workflow, '');

            expect(result).toBe(true);
        });

        it('should return true when workflow requires entities and whitespace selected', () => {
            const workflow = {
                trigger: {
                    form: {
                        entityTypes: [EntityType.Dataset],
                    },
                },
            } as any;

            const result = isEntitySelectionRequired(workflow, '   ');

            expect(result).toBe(true);
        });

        it('should return false when workflow requires entities and valid entity selected', () => {
            const workflow = {
                trigger: {
                    form: {
                        entityTypes: [EntityType.Dataset],
                    },
                },
            } as any;

            const result = isEntitySelectionRequired(workflow, 'urn:li:dataset:test');

            expect(result).toBe(false);
        });

        it('should return false when workflow does not require entities', () => {
            const workflow = {
                trigger: {
                    form: {
                        entityTypes: [],
                    },
                },
            } as any;

            const result = isEntitySelectionRequired(workflow, undefined);

            expect(result).toBe(false);
        });

        it('should return false when entityTypes is undefined', () => {
            const workflow = {} as any;

            const result = isEntitySelectionRequired(workflow, undefined);

            expect(result).toBe(false);
        });
    });
});
