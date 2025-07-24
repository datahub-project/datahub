import { act, renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { useGlobalSettings } from '@app/context/GlobalSettingsContext';
import { useUserContext } from '@app/context/useUserContext';
import { useTemplateState } from '@app/homeV3/context/hooks/useTemplateState';

import { PageTemplateFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, PageTemplateScope, PageTemplateSurfaceType } from '@types';

// Mock the dependencies
vi.mock('@app/context/GlobalSettingsContext', () => ({
    useGlobalSettings: vi.fn(),
}));

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: vi.fn(),
}));

const mockUseGlobalSettings = vi.mocked(useGlobalSettings);
const mockUseUserContext = vi.mocked(useUserContext);

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

describe('useTemplateState', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('initialization', () => {
        it('should initialize with personal template when provided', () => {
            mockUseGlobalSettings.mockReturnValue({
                settings: {
                    globalHomePageSettings: {
                        defaultTemplate: mockGlobalTemplate,
                    },
                },
                loaded: true,
            });
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        homePage: {
                            pageTemplate: mockPersonalTemplate,
                        },
                    },
                },
                loaded: true,
            } as any);

            const { result } = renderHook(() => useTemplateState());

            expect(result.current.personalTemplate).toBe(mockPersonalTemplate);
            expect(result.current.globalTemplate).toBe(mockGlobalTemplate);
            expect(result.current.template).toBe(mockPersonalTemplate);
            expect(result.current.isEditingGlobalTemplate).toBe(false);
        });

        it('should initialize with global template when personal template is null', () => {
            mockUseGlobalSettings.mockReturnValue({
                settings: {
                    globalHomePageSettings: {
                        defaultTemplate: mockGlobalTemplate,
                    },
                },
                loaded: true,
            });
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        homePage: {
                            pageTemplate: null,
                        },
                    },
                },
                loaded: true,
            } as any);

            const { result } = renderHook(() => useTemplateState());

            expect(result.current.personalTemplate).toBe(null);
            expect(result.current.globalTemplate).toBe(mockGlobalTemplate);
            expect(result.current.template).toBe(mockGlobalTemplate);
            expect(result.current.isEditingGlobalTemplate).toBe(false);
        });

        it('should initialize with null when both templates are null', () => {
            mockUseGlobalSettings.mockReturnValue({
                settings: {
                    globalHomePageSettings: {
                        defaultTemplate: null,
                    },
                },
                loaded: true,
            });
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        homePage: {
                            pageTemplate: null,
                        },
                    },
                },
                loaded: true,
            } as any);

            const { result } = renderHook(() => useTemplateState());

            expect(result.current.personalTemplate).toBe(null);
            expect(result.current.globalTemplate).toBe(null);
            expect(result.current.template).toBe(null);
            expect(result.current.isEditingGlobalTemplate).toBe(false);
        });

        it('should handle undefined templates', () => {
            mockUseGlobalSettings.mockReturnValue({
                settings: {
                    globalHomePageSettings: undefined,
                },
                loaded: true,
            });
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        homePage: undefined,
                    },
                },
                loaded: true,
            } as any);

            const { result } = renderHook(() => useTemplateState());

            expect(result.current.personalTemplate).toBe(null);
            expect(result.current.globalTemplate).toBe(null);
            expect(result.current.template).toBe(null);
            expect(result.current.isEditingGlobalTemplate).toBe(false);
        });

        it('should not initialize templates when contexts are not loaded', () => {
            mockUseGlobalSettings.mockReturnValue({
                settings: {
                    globalHomePageSettings: {
                        defaultTemplate: mockGlobalTemplate,
                    },
                },
                loaded: false,
            });
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        homePage: {
                            pageTemplate: mockPersonalTemplate,
                        },
                    },
                },
                loaded: true,
            } as any);

            const { result } = renderHook(() => useTemplateState());

            expect(result.current.personalTemplate).toBe(null);
            expect(result.current.globalTemplate).toBe(null);
            expect(result.current.template).toBe(null);
            expect(result.current.isEditingGlobalTemplate).toBe(false);
        });
    });

    describe('template switching', () => {
        beforeEach(() => {
            mockUseGlobalSettings.mockReturnValue({
                settings: {
                    globalHomePageSettings: {
                        defaultTemplate: mockGlobalTemplate,
                    },
                },
                loaded: true,
            });
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        homePage: {
                            pageTemplate: mockPersonalTemplate,
                        },
                    },
                },
                loaded: true,
            } as any);
        });

        it('should switch to global template when editing global template', () => {
            const { result } = renderHook(() => useTemplateState());

            act(() => {
                result.current.setIsEditingGlobalTemplate(true);
            });

            expect(result.current.isEditingGlobalTemplate).toBe(true);
            expect(result.current.template).toBe(mockGlobalTemplate);
        });

        it('should switch back to personal template when not editing global template', () => {
            const { result } = renderHook(() => useTemplateState());

            act(() => {
                result.current.setIsEditingGlobalTemplate(true);
            });

            act(() => {
                result.current.setIsEditingGlobalTemplate(false);
            });

            expect(result.current.isEditingGlobalTemplate).toBe(false);
            expect(result.current.template).toBe(mockPersonalTemplate);
        });

        it('should use global template when personal template is null and not editing global', () => {
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        homePage: {
                            pageTemplate: null,
                        },
                    },
                },
                loaded: true,
            } as any);

            const { result } = renderHook(() => useTemplateState());

            expect(result.current.template).toBe(mockGlobalTemplate);
        });

        it('should use global template when editing global template even if personal exists', () => {
            const { result } = renderHook(() => useTemplateState());

            act(() => {
                result.current.setIsEditingGlobalTemplate(true);
            });

            expect(result.current.template).toBe(mockGlobalTemplate);
        });
    });

    describe('template updates', () => {
        beforeEach(() => {
            mockUseGlobalSettings.mockReturnValue({
                settings: {
                    globalHomePageSettings: {
                        defaultTemplate: mockGlobalTemplate,
                    },
                },
                loaded: true,
            });
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        homePage: {
                            pageTemplate: mockPersonalTemplate,
                        },
                    },
                },
                loaded: true,
            } as any);
        });

        it('should update personal template when not editing global', () => {
            const { result } = renderHook(() => useTemplateState());

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

            expect(result.current.personalTemplate).toBe(newTemplate);
            expect(result.current.globalTemplate).toBe(mockGlobalTemplate);
            expect(result.current.template).toBe(newTemplate);
        });

        it('should update global template when editing global', () => {
            const { result } = renderHook(() => useTemplateState());

            const newTemplate: PageTemplateFragment = {
                urn: 'urn:li:pageTemplate:new',
                type: EntityType.DatahubPageTemplate,
                properties: {
                    rows: [],
                    surface: { surfaceType: PageTemplateSurfaceType.HomePage },
                    visibility: { scope: PageTemplateScope.Global },
                },
            };

            act(() => {
                result.current.setIsEditingGlobalTemplate(true);
            });

            act(() => {
                result.current.setTemplate(newTemplate);
            });

            expect(result.current.personalTemplate).toEqual(mockPersonalTemplate);
            expect(result.current.globalTemplate).toEqual(newTemplate);
            expect(result.current.template).toEqual(newTemplate);
        });

        it('should update personal template directly', () => {
            const { result } = renderHook(() => useTemplateState());

            const newPersonalTemplate: PageTemplateFragment = {
                urn: 'urn:li:pageTemplate:new',
                type: EntityType.DatahubPageTemplate,
                properties: {
                    rows: [],
                    surface: { surfaceType: PageTemplateSurfaceType.HomePage },
                    visibility: { scope: PageTemplateScope.Personal },
                },
            };

            act(() => {
                result.current.setPersonalTemplate(newPersonalTemplate);
            });

            expect(result.current.personalTemplate).toEqual(newPersonalTemplate);
            expect(result.current.template).toEqual(newPersonalTemplate);
        });

        it('should update global template directly', () => {
            const { result } = renderHook(() => useTemplateState());

            const newTemplate: PageTemplateFragment = {
                urn: 'urn:li:pageTemplate:new',
                type: EntityType.DatahubPageTemplate,
                properties: {
                    rows: [],
                    surface: { surfaceType: PageTemplateSurfaceType.HomePage },
                    visibility: { scope: PageTemplateScope.Global },
                },
            };

            act(() => {
                result.current.setGlobalTemplate(newTemplate);
            });

            expect(result.current.globalTemplate).toEqual(newTemplate);
            expect(result.current.template).toEqual(mockPersonalTemplate); // Still personal since not editing global
        });

        it('should set template to null', () => {
            const { result } = renderHook(() => useTemplateState());

            act(() => {
                result.current.setTemplate(null);
            });

            expect(result.current.personalTemplate).toBe(null);
            expect(result.current.template).toEqual(mockGlobalTemplate); // Falls back to global
        });
    });

    describe('memoization', () => {
        it('should memoize template selection correctly', () => {
            mockUseGlobalSettings.mockReturnValue({
                settings: {
                    globalHomePageSettings: {
                        defaultTemplate: mockGlobalTemplate,
                    },
                },
                loaded: true,
            });
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        homePage: {
                            pageTemplate: mockPersonalTemplate,
                        },
                    },
                },
                loaded: true,
            } as any);

            const { result, rerender } = renderHook(() => useTemplateState());

            const initialTemplate = result.current.template;

            // Rerender
            rerender();

            expect(result.current.template).toBe(initialTemplate);
        });
    });

    describe('edge cases', () => {
        beforeEach(() => {
            mockUseGlobalSettings.mockReturnValue({
                settings: {
                    globalHomePageSettings: {
                        defaultTemplate: mockGlobalTemplate,
                    },
                },
                loaded: true,
            });
        });

        it('should handle switching to global editing when personal template is null', () => {
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        homePage: {
                            pageTemplate: null,
                        },
                    },
                },
                loaded: true,
            } as any);

            const { result } = renderHook(() => useTemplateState());

            act(() => {
                result.current.setIsEditingGlobalTemplate(true);
            });

            expect(result.current.template).toBe(mockGlobalTemplate);
        });

        it('should handle switching back to personal when personal template is null', () => {
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        homePage: {
                            pageTemplate: null,
                        },
                    },
                },
                loaded: true,
            } as any);

            const { result } = renderHook(() => useTemplateState());

            act(() => {
                result.current.setIsEditingGlobalTemplate(true);
            });

            act(() => {
                result.current.setIsEditingGlobalTemplate(false);
            });

            expect(result.current.template).toBe(mockGlobalTemplate); // Falls back to global
        });

        it('should handle setting template to null when editing global', () => {
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        homePage: {
                            pageTemplate: mockPersonalTemplate,
                        },
                    },
                },
                loaded: true,
            } as any);

            const { result } = renderHook(() => useTemplateState());

            act(() => {
                result.current.setIsEditingGlobalTemplate(true);
            });

            act(() => {
                result.current.setTemplate(null);
            });

            expect(result.current.globalTemplate).toBe(null);
            expect(result.current.template).toBe(null);
        });
    });
});
