import { fireEvent, render, screen } from '@testing-library/react';
import { message } from 'antd';
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { deserializeTestDefinition } from '@app/tests/builder/steps/definition/utils';
import { NameStep } from '@app/tests/builder/steps/name/NameStep';
import { StepProps } from '@app/tests/builder/types';
import { validateCompleteTestDefinition } from '@app/tests/builder/validation/utils';
import { TestCategory } from '@app/tests/constants';
import { EntityRegistryContext } from '@src/entityRegistryContext';

vi.mock('@app/tests/builder/validation/utils');
vi.mock('@app/tests/builder/steps/definition/utils');

// Mock antd message to verify info notifications
vi.mock('antd', async () => {
    const actual = await vi.importActual('antd');
    return {
        ...actual,
        message: {
            info: vi.fn(),
            success: vi.fn(),
            error: vi.fn(),
            warning: vi.fn(),
        },
    };
});

vi.mock('@app/tests/builder/steps/name/CategorySelect', () => ({
    CategorySelect: ({ categoryName, onSelect }: any) => (
        <select data-testid="category-select" value={categoryName} onChange={(e) => onSelect(e.target.value)}>
            <option value="METADATA">Metadata</option>
            <option value="CUSTOM">Custom</option>
        </select>
    ),
}));

vi.mock('@app/tests/builder/validation/ValidationWarning', () => ({
    ValidationWarning: ({ warnings }: any) => (
        <div data-testid="validation-warnings">
            {warnings.map((w: any) => (
                <div key={w.message}>{w.message}</div>
            ))}
        </div>
    ),
}));

vi.mock('styled-components', () => {
    const styled = (component: any) => {
        return (_strings: TemplateStringsArray, ..._values: any[]) => component;
    };

    const styledProxy = new Proxy(styled, {
        get: (_target, prop) => {
            return (_strings: TemplateStringsArray, ..._values: any[]) => prop;
        },
    });

    return {
        default: styledProxy,
    };
});

vi.mock('@src/alchemy-components', () => ({
    Button: ({ children, onClick, disabled, ...props }: any) => (
        <button type="button" onClick={onClick} disabled={disabled} {...props}>
            {children}
        </button>
    ),
}));

const mockEntityRegistry = {
    getTypeOrDefaultFromPathName: vi.fn(() => 'DATASET'),
    getTypeFromGraphName: vi.fn((graphName: string) => {
        const map: Record<string, string> = {
            DATASET: 'dataset',
            CHART: 'chart',
            DASHBOARD: 'dashboard',
            GLOSSARY_TERM: 'glossaryTerm',
        };
        return map[graphName] || graphName.toLowerCase();
    }),
} as any;

describe('NameStep', () => {
    let mockState: any;
    let mockUpdateState: ReturnType<typeof vi.fn>;
    let mockGoTo: ReturnType<typeof vi.fn>;
    let mockPrev: ReturnType<typeof vi.fn>;
    let mockSubmit: ReturnType<typeof vi.fn>;
    let mockCancel: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        mockState = {
            name: 'Test Name',
            category: TestCategory.DATA_GOVERNANCE,
            description: 'Test Description',
            definition: {
                json: JSON.stringify({
                    on: { types: ['DATASET'] },
                    rules: [{ property: 'ownership.owners.owner', operator: 'exists' }],
                }),
            },
        };

        mockUpdateState = vi.fn();
        mockGoTo = vi.fn();
        mockPrev = vi.fn();
        mockSubmit = vi.fn();
        mockCancel = vi.fn();

        vi.mocked(deserializeTestDefinition).mockReturnValue({
            on: { types: ['DATASET'] },
            rules: [{ property: 'ownership.owners.owner', operator: 'exists' }],
        });
        vi.mocked(validateCompleteTestDefinition).mockReturnValue([]);

        vi.clearAllMocks();
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    const renderComponent = (props?: Partial<StepProps>) => {
        const defaultProps: StepProps = {
            state: mockState,
            updateState: mockUpdateState,
            goTo: mockGoTo,
            prev: mockPrev,
            submit: mockSubmit,
            cancel: mockCancel,
            ...props,
        };

        return render(
            <EntityRegistryContext.Provider value={mockEntityRegistry}>
                <NameStep {...defaultProps} />
            </EntityRegistryContext.Provider>,
        );
    };

    describe('onClickCreate - Custom Metadata Test Saving', () => {
        it('should call submit when category and name are present with no validation warnings', () => {
            vi.mocked(validateCompleteTestDefinition).mockReturnValue([]);

            renderComponent();

            const saveButton = screen.getByText('Save');
            fireEvent.click(saveButton);

            expect(mockSubmit).toHaveBeenCalledTimes(1);
            expect(message.info).not.toHaveBeenCalled();
        });

        it('should allow saving custom metadata tests with validation warnings and show info message', () => {
            const mockWarnings = [
                {
                    type: 'property' as const,
                    message: 'Property "customField" may not be compatible with selected entity types',
                    propertyId: 'customField',
                },
                {
                    type: 'action' as const,
                    message: 'Action "Custom Action" may not be compatible with selected entity types',
                    actionId: 'CUSTOM_ACTION' as any,
                },
            ];

            vi.mocked(validateCompleteTestDefinition).mockReturnValue(mockWarnings);

            renderComponent();

            expect(screen.getByTestId('validation-warnings')).toBeInTheDocument();

            const saveButton = screen.getByText('Save');
            fireEvent.click(saveButton);

            expect(message.info).toHaveBeenCalledTimes(1);
            expect(message.info).toHaveBeenCalledWith(
                'Saving test with validation warnings. Some properties may not be compatible with your selected entity types. You can edit the test later to fix any issues.',
                5,
            );

            expect(mockSubmit).toHaveBeenCalledTimes(1);
        });

        it('should allow saving with multiple validation warnings', () => {
            const mockWarnings = [
                {
                    type: 'property' as const,
                    message: 'Property "datasetProperties.description" not supported for glossary terms',
                    propertyId: 'datasetProperties.description',
                },
                {
                    type: 'property' as const,
                    message: 'Property "schemaMetadata.fields.fieldPath" not supported for charts',
                    propertyId: 'schemaMetadata.fields.fieldPath',
                },
                {
                    type: 'action' as const,
                    message: 'Action "Add Tags" not supported for glossary terms',
                    actionId: 'ADD_TAGS' as any,
                },
            ];

            vi.mocked(validateCompleteTestDefinition).mockReturnValue(mockWarnings);

            renderComponent();

            const saveButton = screen.getByText('Save');
            fireEvent.click(saveButton);

            expect(message.info).toHaveBeenCalledTimes(1);
            expect(mockSubmit).toHaveBeenCalledTimes(1);
        });

        it('should not call submit when name is missing', () => {
            const stateWithoutName = {
                ...mockState,
                name: '',
            };

            renderComponent({ state: stateWithoutName });

            const saveButton = screen.getByText('Save');
            fireEvent.click(saveButton);

            expect(mockSubmit).not.toHaveBeenCalled();
            expect(message.info).not.toHaveBeenCalled();
        });

        it('should not call submit when category is missing', () => {
            const stateWithoutCategory = {
                ...mockState,
                category: '',
            };

            renderComponent({ state: stateWithoutCategory });

            const saveButton = screen.getByText('Save');
            fireEvent.click(saveButton);

            expect(mockSubmit).not.toHaveBeenCalled();
            expect(message.info).not.toHaveBeenCalled();
        });

        it('should not show info message when there are no validation warnings', () => {
            vi.mocked(validateCompleteTestDefinition).mockReturnValue([]);

            renderComponent();

            const saveButton = screen.getByText('Save');
            fireEvent.click(saveButton);

            expect(message.info).not.toHaveBeenCalled();
            expect(mockSubmit).toHaveBeenCalledTimes(1);
        });

        it('should validate and allow saving custom category tests with warnings', () => {
            const customCategoryState = {
                ...mockState,
                category: 'CUSTOM_METADATA_TYPE',
            };

            const mockWarnings = [
                {
                    type: 'property' as const,
                    message: 'Custom property may not be supported',
                    propertyId: 'customProperty',
                },
            ];

            vi.mocked(validateCompleteTestDefinition).mockReturnValue(mockWarnings);

            renderComponent({ state: customCategoryState });

            const saveButton = screen.getByText('Save');
            fireEvent.click(saveButton);

            expect(message.info).toHaveBeenCalledTimes(1);
            expect(mockSubmit).toHaveBeenCalledTimes(1);
        });

        it('should handle edge case with name but undefined category', () => {
            const stateWithUndefinedCategory = {
                ...mockState,
                category: undefined,
            };

            renderComponent({ state: stateWithUndefinedCategory });

            const saveButton = screen.getByText('Save');
            fireEvent.click(saveButton);

            expect(mockSubmit).not.toHaveBeenCalled();
        });

        it('should handle edge case with category but undefined name', () => {
            const stateWithUndefinedName = {
                ...mockState,
                name: undefined,
            };

            renderComponent({ state: stateWithUndefinedName });

            const saveButton = screen.getByText('Save');
            fireEvent.click(saveButton);

            expect(mockSubmit).not.toHaveBeenCalled();
        });
    });

    describe('Save button state', () => {
        it('should disable save button when name is empty', () => {
            const stateWithoutName = {
                ...mockState,
                name: '',
            };

            renderComponent({ state: stateWithoutName });

            const saveButton = screen.getByText('Save');
            expect(saveButton).toBeDisabled();
        });

        it('should enable save button when name is present', () => {
            renderComponent();

            const saveButton = screen.getByText('Save');
            expect(saveButton).not.toBeDisabled();
        });

        it('should disable save button when name is undefined', () => {
            const stateWithUndefinedName = {
                ...mockState,
                name: undefined,
            };

            renderComponent({ state: stateWithUndefinedName });

            const saveButton = screen.getByText('Save');
            expect(saveButton).toBeDisabled();
        });
    });

    describe('Validation warnings display', () => {
        it('should display validation warnings when present', () => {
            const mockWarnings = [
                {
                    type: 'property' as const,
                    message: 'Test warning message',
                    propertyId: 'testProperty',
                },
            ];

            vi.mocked(validateCompleteTestDefinition).mockReturnValue(mockWarnings);

            renderComponent();

            expect(screen.getByTestId('validation-warnings')).toBeInTheDocument();
            expect(screen.getByText('Test warning message')).toBeInTheDocument();
        });

        it('should not display validation warnings section when no warnings', () => {
            vi.mocked(validateCompleteTestDefinition).mockReturnValue([]);

            renderComponent();

            expect(screen.queryByTestId('validation-warnings')).not.toBeInTheDocument();
        });
    });
});
