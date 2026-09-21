import { act, renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { useGlobalSettings } from '@app/context/GlobalSettingsContext';
import { useUserContext } from '@app/context/useUserContext';
import { useEntityContext } from '@app/entity/shared/EntityContext';
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

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityContext: vi.fn(),
}));

vi.mock('@app/shared/usePrevious', () => ({
    default: vi.fn(),
}));

const mockUseGlobalSettings = vi.mocked(useGlobalSettings);
const mockUseUserContext = vi.mocked(useUserContext);
const mockUseEntityContext = vi.mocked(useEntityContext);
const mockUsePrevious = vi.mocked(await import('@app/shared/usePrevious')).default;

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
                        exists: true,
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
                        exists: true,
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
        mockUseEntityContext.mockReturnValue({
            urn: '',
            entityType: EntityType.Dataset,
            entityData: null,
            loading: false,
            baseEntity: null,
            updateEntity: vi.fn(),
            routeToTab: vi.fn(),
            refetch: vi.fn(),
            lineage: undefined,
            dataNotCombinedWithSiblings: null,
            entityState: { shouldRefetchContents: false, setShouldRefetchContents: vi.fn() },
        });
        mockUsePrevious.mockReturnValue(undefined);
    });

    afterEach(() => {
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

            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

            expect(result.current.personalTemplate).toStrictEqual(mockPersonalTemplate);
            expect(result.current.globalTemplate).toStrictEqual(mockGlobalTemplate);
            expect(result.current.template).toStrictEqual(mockPersonalTemplate);
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

            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

            expect(result.current.personalTemplate).toBe(null);
            expect(result.current.globalTemplate).toStrictEqual(mockGlobalTemplate);
            expect(result.current.template).toStrictEqual(mockGlobalTemplate);
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

            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

            expect(result.current.personalTemplate).toBe(null);
            // undefined global template should be replaced with fallback default global template
            expect(result.current.globalTemplate?.urn).toBe('urn:li:dataHubPageTemplate:home_default_1');
            expect(result.current.template?.urn).toBe('urn:li:dataHubPageTemplate:home_default_1');
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

            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

            expect(result.current.personalTemplate).toBe(null);
            // undefined global template should be replaced with fallback default global template
            expect(result.current.globalTemplate?.urn).toBe('urn:li:dataHubPageTemplate:home_default_1');
            expect(result.current.template?.urn).toBe('urn:li:dataHubPageTemplate:home_default_1');
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

            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

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
            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

            act(() => {
                result.current.setIsEditingGlobalTemplate(true);
            });

            expect(result.current.isEditingGlobalTemplate).toBe(true);
            expect(result.current.template).toStrictEqual(mockGlobalTemplate);
        });

        it('should switch back to personal template when not editing global template', () => {
            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

            act(() => {
                result.current.setIsEditingGlobalTemplate(true);
            });

            act(() => {
                result.current.setIsEditingGlobalTemplate(false);
            });

            expect(result.current.isEditingGlobalTemplate).toBe(false);
            expect(result.current.template).toStrictEqual(mockPersonalTemplate);
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

            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

            expect(result.current.template).toStrictEqual(mockGlobalTemplate);
        });

        it('should use global template when editing global template even if personal exists', () => {
            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

            act(() => {
                result.current.setIsEditingGlobalTemplate(true);
            });

            expect(result.current.template).toStrictEqual(mockGlobalTemplate);
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
            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

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
            expect(result.current.globalTemplate).toStrictEqual(mockGlobalTemplate);
            expect(result.current.template).toBe(newTemplate);
        });

        it('should update global template when editing global', () => {
            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

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
            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

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
            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

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
            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

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

            const { result, rerender } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

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

            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

            act(() => {
                result.current.setIsEditingGlobalTemplate(true);
            });

            expect(result.current.template).toStrictEqual(mockGlobalTemplate);
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

            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

            act(() => {
                result.current.setIsEditingGlobalTemplate(true);
            });

            act(() => {
                result.current.setIsEditingGlobalTemplate(false);
            });

            expect(result.current.template).toStrictEqual(mockGlobalTemplate); // Falls back to global
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

            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.HomePage));

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

    describe('AssetSummary template initialization', () => {
        const mockAssetSummaryTemplate: PageTemplateFragment = {
            urn: 'urn:li:pageTemplate:assetSummary',
            type: EntityType.DatahubPageTemplate,
            properties: {
                rows: [
                    {
                        modules: [
                            {
                                urn: 'urn:li:pageModule:3',
                                type: EntityType.DatahubPageModule,
                                exists: true,
                                properties: {
                                    name: 'Asset Summary Module',
                                    type: DataHubPageModuleType.Link,
                                    visibility: { scope: PageModuleScope.Global },
                                    params: {},
                                },
                            },
                        ],
                    },
                ],
                surface: { surfaceType: PageTemplateSurfaceType.AssetSummary },
                visibility: { scope: PageTemplateScope.Global },
            },
        };

        it('should initialize AssetSummary template with default and personal templates when entityData is available', () => {
            mockUseEntityContext.mockReturnValue({
                urn: 'test-urn',
                entityType: EntityType.Dataset,
                entityData: {
                    urn: 'test-urn',
                    settings: {
                        assetSummary: {
                            templates: [
                                {
                                    template: mockAssetSummaryTemplate as any,
                                },
                            ],
                        },
                    },
                },
                loading: false,
                baseEntity: null,
                updateEntity: vi.fn(),
                routeToTab: vi.fn(),
                refetch: vi.fn(),
                lineage: undefined,
                dataNotCombinedWithSiblings: null,
                entityState: { shouldRefetchContents: false, setShouldRefetchContents: vi.fn() },
            });
            mockUsePrevious.mockReturnValue(undefined);
            mockUseGlobalSettings.mockReturnValue({
                settings: {},
                loaded: true,
            });
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        assetSummary: {
                            templates: [
                                {
                                    template: mockAssetSummaryTemplate,
                                },
                            ],
                        },
                    },
                },
                loaded: true,
            } as any);

            const { result } = renderHook(() => useTemplateState(PageTemplateSurfaceType.AssetSummary));

            expect(result.current.globalTemplate?.urn).toBe('urn:li:dataHubPageTemplate:asset_summary_default');
            expect(result.current.personalTemplate?.urn).toBe('urn:li:pageTemplate:assetSummary');
        });

        it('should not reinitialize AssetSummary templates when urn does not change', () => {
            mockUseEntityContext.mockReturnValue({
                urn: 'test-urn',
                entityType: EntityType.Dataset,
                entityData: {
                    urn: 'test-urn',
                    settings: {
                        assetSummary: {
                            templates: [
                                {
                                    template: mockAssetSummaryTemplate as any,
                                },
                            ],
                        },
                    },
                },
                loading: false,
                baseEntity: null,
                updateEntity: vi.fn(),
                routeToTab: vi.fn(),
                refetch: vi.fn(),
                lineage: undefined,
                dataNotCombinedWithSiblings: null,
                entityState: { shouldRefetchContents: false, setShouldRefetchContents: vi.fn() },
            });
            mockUseGlobalSettings.mockReturnValue({
                settings: {},
                loaded: true,
            });
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        assetSummary: {
                            templates: [
                                {
                                    template: mockAssetSummaryTemplate,
                                },
                            ],
                        },
                    },
                },
                loaded: true,
            } as any);

            mockUsePrevious.mockReturnValue('test-urn');

            const { result, rerender } = renderHook(() => useTemplateState(PageTemplateSurfaceType.AssetSummary));

            const initialGlobalTemplate = result.current.globalTemplate;
            const initialPersonalTemplate = result.current.personalTemplate;

            rerender();

            expect(result.current.globalTemplate).toBe(initialGlobalTemplate);
            expect(result.current.personalTemplate).toBe(initialPersonalTemplate);
        });

        it('should reinitialize AssetSummary templates when urn changes', () => {
            let currentUrn = 'old-urn';

            mockUseEntityContext.mockReturnValue({
                urn: currentUrn,
                entityType: EntityType.Dataset,
                entityData: {
                    urn: currentUrn,
                    settings: {
                        assetSummary: {
                            templates: [
                                {
                                    template: mockAssetSummaryTemplate as any,
                                },
                            ],
                        },
                    },
                },
                loading: false,
                baseEntity: null,
                updateEntity: vi.fn(),
                routeToTab: vi.fn(),
                refetch: vi.fn(),
                lineage: undefined,
                dataNotCombinedWithSiblings: null,
                entityState: { shouldRefetchContents: false, setShouldRefetchContents: vi.fn() },
            });
            mockUseGlobalSettings.mockReturnValue({
                settings: {},
                loaded: true,
            });
            mockUseUserContext.mockReturnValue({
                user: {
                    settings: {
                        assetSummary: {
                            templates: [
                                {
                                    template: mockAssetSummaryTemplate,
                                },
                            ],
                        },
                    },
                },
                loaded: true,
            } as any);

            // First render - initialize with old URN
            mockUsePrevious.mockReturnValue(undefined);
            const { result, rerender } = renderHook(() => useTemplateState(PageTemplateSurfaceType.AssetSummary));

            // Verify templates are initialized
            expect(result.current.globalTemplate).not.toBeNull();
            expect(result.current.personalTemplate).not.toBeNull();
            const initialPersonalTemplateUrn = result.current.personalTemplate?.urn;

            // Simulate URN change - prevUrn becomes old-urn, current urn becomes new-urn
            currentUrn = 'new-urn';

            // Change the personal template URN to verify reinitialization picks up the new one
            const newAssetSummaryTemplate: PageTemplateFragment = {
                ...mockAssetSummaryTemplate,
                urn: 'urn:li:pageTemplate:assetSummary-new',
            };

            mockUseEntityContext.mockReturnValue({
                urn: currentUrn,
                entityType: EntityType.Dataset,
                entityData: {
                    urn: currentUrn,
                    settings: {
                        assetSummary: {
                            templates: [
                                {
                                    template: newAssetSummaryTemplate as any,
                                },
                            ],
                        },
                    },
                },
                loading: false,
                baseEntity: null,
                updateEntity: vi.fn(),
                routeToTab: vi.fn(),
                refetch: vi.fn(),
                lineage: undefined,
                dataNotCombinedWithSiblings: null,
                entityState: { shouldRefetchContents: false, setShouldRefetchContents: vi.fn() },
            });
            mockUsePrevious.mockReturnValue('old-urn');

            rerender();

            // Templates should be reinitialized with new entity's template
            expect(result.current.globalTemplate).not.toBeNull();
            expect(result.current.personalTemplate?.urn).toBe('urn:li:pageTemplate:assetSummary-new');

            // Verify that personal template was reinitialized (different URN)
            expect(result.current.personalTemplate?.urn).not.toBe(initialPersonalTemplateUrn);
        });
    });
});
