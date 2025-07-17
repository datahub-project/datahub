import { act, render } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { vi } from 'vitest';

import { PageTemplateProvider, usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { useModuleOperations } from '@app/homeV3/context/hooks/useModuleOperations';
import { useTemplateOperations } from '@app/homeV3/context/hooks/useTemplateOperations';
import { useTemplateState } from '@app/homeV3/context/hooks/useTemplateState';

import { PageTemplateFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, PageTemplateScope, PageTemplateSurfaceType } from '@types';

// Mock the hooks before importing the component
vi.mock('../hooks/useTemplateState', () => ({
    useTemplateState: vi.fn(),
}));

vi.mock('../hooks/useTemplateOperations', () => ({
    useTemplateOperations: vi.fn(),
}));

vi.mock('../hooks/useModuleOperations', () => ({
    useModuleOperations: vi.fn(),
}));

// Get the mocked functions
const mockUseTemplateState = vi.mocked(useTemplateState);
const mockUseTemplateOperations = vi.mocked(useTemplateOperations);
const mockUseModuleOperations = vi.mocked(useModuleOperations);

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
                            type: DataHubPageModuleType.Link,
                            visibility: { scope: PageModuleScope.Personal },
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
                            type: DataHubPageModuleType.Link,
                            visibility: { scope: PageModuleScope.Global },
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
const mockSetIsEditingGlobalTemplate = vi.fn();
const mockSetPersonalTemplate = vi.fn();
const mockSetGlobalTemplate = vi.fn();
const mockSetTemplate = vi.fn();
const mockAddModule = vi.fn();
const mockRemoveModule = vi.fn();
const mockCreateModule = vi.fn();
const mockUpdateTemplateWithModule = vi.fn();
const mockRemoveModuleFromTemplate = vi.fn();
const mockUpsertTemplate = vi.fn();

describe('PageTemplateContext', () => {
    beforeEach(() => {
        vi.clearAllMocks();

        // Setup default mock returns
        mockUseTemplateState.mockReturnValue({
            personalTemplate: mockPersonalTemplate,
            globalTemplate: mockGlobalTemplate,
            template: mockPersonalTemplate,
            isEditingGlobalTemplate: false,
            setIsEditingGlobalTemplate: mockSetIsEditingGlobalTemplate,
            setPersonalTemplate: mockSetPersonalTemplate,
            setGlobalTemplate: mockSetGlobalTemplate,
            setTemplate: mockSetTemplate,
        });

        mockUseTemplateOperations.mockReturnValue({
            updateTemplateWithModule: mockUpdateTemplateWithModule,
            removeModuleFromTemplate: mockRemoveModuleFromTemplate,
            upsertTemplate: mockUpsertTemplate,
        });

        mockUseModuleOperations.mockReturnValue({
            addModule: mockAddModule,
            removeModule: mockRemoveModule,
            createModule: mockCreateModule,
        });
    });

    describe('PageTemplateProvider', () => {
        it('should provide context with all required values', () => {
            const TestComponent = () => {
                const context = usePageTemplateContext();
                return (
                    <div>
                        <span data-testid="personal-template">{context.personalTemplate?.urn}</span>
                        <span data-testid="global-template">{context.globalTemplate?.urn}</span>
                        <span data-testid="template">{context.template?.urn}</span>
                        <span data-testid="is-editing-global">{context.isEditingGlobalTemplate.toString()}</span>
                    </div>
                );
            };

            const { getByTestId } = render(
                <PageTemplateProvider personalTemplate={mockPersonalTemplate} globalTemplate={mockGlobalTemplate}>
                    <TestComponent />
                </PageTemplateProvider>,
            );

            expect(getByTestId('personal-template')).toHaveTextContent('urn:li:pageTemplate:personal');
            expect(getByTestId('global-template')).toHaveTextContent('urn:li:pageTemplate:global');
            expect(getByTestId('template')).toHaveTextContent('urn:li:pageTemplate:personal');
            expect(getByTestId('is-editing-global')).toHaveTextContent('false');
        });

        it('should call useTemplateState with correct parameters', () => {
            render(
                <PageTemplateProvider personalTemplate={mockPersonalTemplate} globalTemplate={mockGlobalTemplate}>
                    <div>Test</div>
                </PageTemplateProvider>,
            );

            expect(mockUseTemplateState).toHaveBeenCalledWith(mockPersonalTemplate, mockGlobalTemplate);
        });

        it('should call useTemplateOperations', () => {
            render(
                <PageTemplateProvider personalTemplate={mockPersonalTemplate} globalTemplate={mockGlobalTemplate}>
                    <div>Test</div>
                </PageTemplateProvider>,
            );

            expect(mockUseTemplateOperations).toHaveBeenCalled();
        });

        it('should call useModuleOperations with correct parameters', () => {
            render(
                <PageTemplateProvider personalTemplate={mockPersonalTemplate} globalTemplate={mockGlobalTemplate}>
                    <div>Test</div>
                </PageTemplateProvider>,
            );

            expect(mockUseModuleOperations).toHaveBeenCalledWith(
                false, // isEditingGlobalTemplate
                mockPersonalTemplate,
                mockGlobalTemplate,
                mockSetPersonalTemplate,
                mockSetGlobalTemplate,
                mockUpdateTemplateWithModule,
                mockRemoveModuleFromTemplate,
                mockUpsertTemplate,
            );
        });

        it('should handle null templates', () => {
            mockUseTemplateState.mockReturnValue({
                personalTemplate: null,
                globalTemplate: null,
                template: null,
                isEditingGlobalTemplate: false,
                setIsEditingGlobalTemplate: mockSetIsEditingGlobalTemplate,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                setTemplate: mockSetTemplate,
            });

            const TestComponent = () => {
                const context = usePageTemplateContext();
                return (
                    <div>
                        <span data-testid="personal-template">{context.personalTemplate?.urn || 'null'}</span>
                        <span data-testid="global-template">{context.globalTemplate?.urn || 'null'}</span>
                        <span data-testid="template">{context.template?.urn || 'null'}</span>
                    </div>
                );
            };

            const { getByTestId } = render(
                <PageTemplateProvider personalTemplate={null} globalTemplate={null}>
                    <TestComponent />
                </PageTemplateProvider>,
            );

            expect(getByTestId('personal-template')).toHaveTextContent('null');
            expect(getByTestId('global-template')).toHaveTextContent('null');
            expect(getByTestId('template')).toHaveTextContent('null');
        });

        it('should handle undefined templates', () => {
            mockUseTemplateState.mockReturnValue({
                personalTemplate: null,
                globalTemplate: null,
                template: null,
                isEditingGlobalTemplate: false,
                setIsEditingGlobalTemplate: mockSetIsEditingGlobalTemplate,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                setTemplate: mockSetTemplate,
            });

            const TestComponent = () => {
                const context = usePageTemplateContext();
                return (
                    <div>
                        <span data-testid="personal-template">{context.personalTemplate?.urn || 'null'}</span>
                        <span data-testid="global-template">{context.globalTemplate?.urn || 'null'}</span>
                        <span data-testid="template">{context.template?.urn || 'null'}</span>
                    </div>
                );
            };

            const { getByTestId } = render(
                <PageTemplateProvider personalTemplate={undefined} globalTemplate={undefined}>
                    <TestComponent />
                </PageTemplateProvider>,
            );

            expect(getByTestId('personal-template')).toHaveTextContent('null');
            expect(getByTestId('global-template')).toHaveTextContent('null');
            expect(getByTestId('template')).toHaveTextContent('null');
        });

        it('should update module operations when editing global template', () => {
            mockUseTemplateState.mockReturnValue({
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
                template: mockGlobalTemplate,
                isEditingGlobalTemplate: true,
                setIsEditingGlobalTemplate: mockSetIsEditingGlobalTemplate,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                setTemplate: mockSetTemplate,
            });

            render(
                <PageTemplateProvider personalTemplate={mockPersonalTemplate} globalTemplate={mockGlobalTemplate}>
                    <div>Test</div>
                </PageTemplateProvider>,
            );

            expect(useModuleOperations).toHaveBeenCalledWith(
                true, // isEditingGlobalTemplate
                mockPersonalTemplate,
                mockGlobalTemplate,
                mockSetPersonalTemplate,
                mockSetGlobalTemplate,
                mockUpdateTemplateWithModule,
                mockRemoveModuleFromTemplate,
                mockUpsertTemplate,
            );
        });

        it('should memoize context value correctly', () => {
            const TestComponent = () => {
                const context = usePageTemplateContext();
                return <div data-testid="context">{JSON.stringify(context.template?.urn)}</div>;
            };

            const { getByTestId, rerender } = render(
                <PageTemplateProvider personalTemplate={mockPersonalTemplate} globalTemplate={mockGlobalTemplate}>
                    <TestComponent />
                </PageTemplateProvider>,
            );

            const initialValue = getByTestId('context').textContent;

            // Rerender with same props
            rerender(
                <PageTemplateProvider personalTemplate={mockPersonalTemplate} globalTemplate={mockGlobalTemplate}>
                    <TestComponent />
                </PageTemplateProvider>,
            );

            expect(getByTestId('context').textContent).toBe(initialValue);
        });
    });

    describe('usePageTemplateContext', () => {
        it('should return context when used within provider', () => {
            const { result } = renderHook(() => usePageTemplateContext(), {
                wrapper: ({ children }) => (
                    <PageTemplateProvider personalTemplate={mockPersonalTemplate} globalTemplate={mockGlobalTemplate}>
                        {children}
                    </PageTemplateProvider>
                ),
            });

            expect(result.current.personalTemplate).toBe(mockPersonalTemplate);
            expect(result.current.globalTemplate).toBe(mockGlobalTemplate);
            expect(result.current.template).toBe(mockPersonalTemplate);
            expect(result.current.isEditingGlobalTemplate).toBe(false);
            expect(result.current.setIsEditingGlobalTemplate).toBe(mockSetIsEditingGlobalTemplate);
            expect(result.current.setPersonalTemplate).toBe(mockSetPersonalTemplate);
            expect(result.current.setGlobalTemplate).toBe(mockSetGlobalTemplate);
            expect(result.current.setTemplate).toBe(mockSetTemplate);
            expect(result.current.addModule).toBe(mockAddModule);
            expect(result.current.createModule).toBe(mockCreateModule);
        });

        it('should throw error when used outside provider', () => {
            const { result } = renderHook(() => usePageTemplateContext());

            expect(result.error).toEqual(
                new Error('usePageTemplateContext must be used within a PageTemplateProvider'),
            );
        });

        it('should provide working addModule function', () => {
            const { result } = renderHook(() => usePageTemplateContext(), {
                wrapper: ({ children }) => (
                    <PageTemplateProvider personalTemplate={mockPersonalTemplate} globalTemplate={mockGlobalTemplate}>
                        {children}
                    </PageTemplateProvider>
                ),
            });

            const moduleInput = {
                module: {
                    urn: 'urn:li:pageModule:test',
                    type: EntityType.DatahubPageModule,
                    properties: {
                        name: 'Test Module',
                        type: DataHubPageModuleType.Link,
                        visibility: { scope: PageModuleScope.Personal },
                        params: {},
                    },
                },
                position: {
                    rowIndex: 0,
                    rowSide: 'left' as const,
                },
            };

            act(() => {
                result.current.addModule(moduleInput);
            });

            expect(mockAddModule).toHaveBeenCalledWith(moduleInput);
        });

        it('should provide working createModule function', () => {
            const { result } = renderHook(() => usePageTemplateContext(), {
                wrapper: ({ children }) => (
                    <PageTemplateProvider personalTemplate={mockPersonalTemplate} globalTemplate={mockGlobalTemplate}>
                        {children}
                    </PageTemplateProvider>
                ),
            });

            const createModuleInput = {
                name: 'New Module',
                type: DataHubPageModuleType.Link,
                scope: PageModuleScope.Personal,
                params: { limit: 10 },
                position: {
                    rowIndex: 0,
                    rowSide: 'left' as const,
                },
            };

            act(() => {
                result.current.createModule(createModuleInput);
            });

            expect(mockCreateModule).toHaveBeenCalledWith(createModuleInput);
        });

        it('should provide working setIsEditingGlobalTemplate function', () => {
            const { result } = renderHook(() => usePageTemplateContext(), {
                wrapper: ({ children }) => (
                    <PageTemplateProvider personalTemplate={mockPersonalTemplate} globalTemplate={mockGlobalTemplate}>
                        {children}
                    </PageTemplateProvider>
                ),
            });

            act(() => {
                result.current.setIsEditingGlobalTemplate(true);
            });

            expect(mockSetIsEditingGlobalTemplate).toHaveBeenCalledWith(true);
        });

        it('should provide working setTemplate function', () => {
            const { result } = renderHook(() => usePageTemplateContext(), {
                wrapper: ({ children }) => (
                    <PageTemplateProvider personalTemplate={mockPersonalTemplate} globalTemplate={mockGlobalTemplate}>
                        {children}
                    </PageTemplateProvider>
                ),
            });

            const newTemplate: PageTemplateFragment = {
                urn: 'urn:li:pageTemplate:new',
                type: EntityType.DatahubPageTemplate,
                properties: {
                    rows: [],
                    surface: { surfaceType: PageTemplateSurfaceType.HomePage },
                    visibility: { scope: PageTemplateScope.Personal },
                },
            };

            act(() => {
                result.current.setTemplate(newTemplate);
            });

            expect(mockSetTemplate).toHaveBeenCalledWith(newTemplate);
        });
    });

    describe('context value stability', () => {
        it('should maintain stable context value between renders', () => {
            const TestComponent = () => {
                const context = usePageTemplateContext();
                return <div data-testid="context">{context.template?.urn}</div>;
            };

            const { getByTestId, rerender } = render(
                <PageTemplateProvider personalTemplate={mockPersonalTemplate} globalTemplate={mockGlobalTemplate}>
                    <TestComponent />
                </PageTemplateProvider>,
            );

            // Update the mock to return different values
            mockUseTemplateState.mockReturnValue({
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
                template: mockGlobalTemplate, // Changed from personal to global
                isEditingGlobalTemplate: true, // Changed from false to true
                setIsEditingGlobalTemplate: mockSetIsEditingGlobalTemplate,
                setPersonalTemplate: mockSetPersonalTemplate,
                setGlobalTemplate: mockSetGlobalTemplate,
                setTemplate: mockSetTemplate,
            });

            rerender(
                <PageTemplateProvider personalTemplate={mockPersonalTemplate} globalTemplate={mockGlobalTemplate}>
                    <TestComponent />
                </PageTemplateProvider>,
            );

            // The context value should update to reflect the new template
            expect(getByTestId('context').textContent).toBe('urn:li:pageTemplate:global');
        });
    });
});
