import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useAddModuleMenu from '@app/homeV3/template/components/addModuleMenu/useAddModuleMenu';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, PageTemplateSurfaceType } from '@types';

// Mock the PageTemplateContext
const mockAddModule = vi.fn();
const mockOpenModal = vi.fn();

// Mock template and globalTemplate data - using any to avoid complex type issues
const mockTemplate = {
    properties: {
        rows: [
            {
                modules: [
                    {
                        properties: {
                            type: DataHubPageModuleType.Link,
                        },
                    },
                ],
            },
        ],
    },
} as any;

const mockGlobalTemplate = {
    properties: {
        rows: [
            {
                modules: [
                    {
                        urn: 'urn:li:dataHubPageModule:admin1',
                        type: EntityType.DatahubPageModule,
                        properties: {
                            name: 'Admin Widget 1',
                            type: DataHubPageModuleType.Link,
                            visibility: { scope: PageModuleScope.Global },
                            params: {},
                        },
                    } as PageModuleFragment,
                    {
                        urn: 'urn:li:dataHubPageModule:admin2',
                        type: EntityType.DatahubPageModule,
                        properties: {
                            name: 'Admin Widget 2',
                            type: DataHubPageModuleType.RichText,
                            visibility: { scope: PageModuleScope.Global },
                            params: {},
                        },
                    } as PageModuleFragment,
                ],
            },
        ],
    },
} as any;

const mockEmptyGlobalTemplate = {
    properties: {
        rows: [],
    },
} as any;

// Mock function for usePageTemplateContext
const { mockUsePageTemplateContext } = vi.hoisted(() => {
    return {
        mockUsePageTemplateContext: vi.fn(),
    };
});

// Mock function for useEntityData
const { mockUseEntityData } = vi.hoisted(() => {
    return {
        mockUseEntityData: vi.fn(),
    };
});

vi.mock('@app/homeV3/context/PageTemplateContext', () => ({
    usePageTemplateContext: mockUsePageTemplateContext,
}));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: mockUseEntityData,
}));

// Mock components that are rendered inside the menu items
vi.mock('@app/homeV3/template/components/addModuleMenu/components/GroupItem', () => ({
    __esModule: true,
    default: ({ title }: { title: string }) => <div data-testid="group-item">{title}</div>,
}));

vi.mock('@app/homeV3/template/components/addModuleMenu/components/ModuleMenuItem', () => ({
    __esModule: true,
    default: ({ module }: { module: any }) => <div data-testid={`module-${module.key}`}>{module.name}</div>,
}));

vi.mock('@app/homeV3/template/components/addModuleMenu/components/MenuItem', () => ({
    __esModule: true,
    default: ({ title }: { title: string }) => <div data-testid="menu-item">{title}</div>,
}));

describe('useAddModuleMenu', () => {
    const mockCloseMenu = vi.fn();
    const mockPosition: ModulePositionInput = { rowIndex: 0, rowSide: 'left' };

    beforeEach(() => {
        vi.clearAllMocks();
        // Set up default mock implementation
        mockUseEntityData.mockReturnValue({
            entityType: EntityType.Domain,
        });
        mockUsePageTemplateContext.mockReturnValue({
            addModule: mockAddModule,
            moduleModalState: {
                open: mockOpenModal,
                close: vi.fn(),
                isOpen: false,
                isEditing: false,
            },
            template: mockTemplate,
            globalTemplate: mockEmptyGlobalTemplate,
            templateType: PageTemplateSurfaceType.HomePage,
        });
    });

    function getChildren(item: any): any[] {
        if (item && 'children' in item && Array.isArray(item.children)) return item.children;
        return [];
    }

    it('should return menu items with correct structure for HomePage with Domain entity (no global template)', () => {
        const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));

        const { items } = result.current;
        expect(items).toHaveLength(2);

        // Check "Create Your Own" group - HomePage should have quickLink
        expect(items?.[0]).toHaveProperty('key', 'customModulesGroup');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[0]?.children).toHaveLength(4);
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[0]?.children?.[0]).toHaveProperty('key', 'quick-link');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[0]?.children?.[1]).toHaveProperty('key', 'asset-collection');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[0]?.children?.[2]).toHaveProperty('key', 'documentation');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[0]?.children?.[3]).toHaveProperty('key', 'hierarchyView');

        // Check "Default" group - HomePage should have yourAssets and domains
        expect(items?.[1]).toHaveProperty('key', 'customLargeModulesGroup');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[1]?.children).toHaveLength(3);
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[1]?.children?.[0]).toHaveProperty('key', 'your-assets');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[1]?.children?.[1]).toHaveProperty('key', 'domains');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[1]?.children?.[2]).toHaveProperty('key', 'platforms');
    });

    it('should include admin created modules when available in global template for HomePage', () => {
        // Mock the context to return globalTemplate with custom modules
        mockUsePageTemplateContext.mockReturnValue({
            addModule: mockAddModule,
            moduleModalState: {
                open: mockOpenModal,
                close: vi.fn(),
                isOpen: false,
                isEditing: false,
            },
            template: mockTemplate,
            globalTemplate: mockGlobalTemplate,
            templateType: PageTemplateSurfaceType.HomePage,
        });

        const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));

        const { items } = result.current;
        expect(items).toHaveLength(3);

        // Check "Shared" group contains home defaults
        expect(items?.[2]).toHaveProperty('key', 'sharedModulesGroup');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[2]?.children).toHaveLength(1);
        // @ts-expect-error SubMenuItem should have children
        const homeDefaults = items?.[2]?.children?.[0];
        expect(homeDefaults).toHaveProperty('key', 'adminCreatedModulesGroup');
        expect(homeDefaults?.children).toHaveLength(2);
        expect(homeDefaults?.children?.[0]).toHaveProperty('key', 'urn:li:dataHubPageModule:admin1');
        expect(homeDefaults?.children?.[1]).toHaveProperty('key', 'urn:li:dataHubPageModule:admin2');
    });

    it('should call addModule and closeMenu when Your Assets is clicked', () => {
        const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));

        // @ts-expect-error SubMenuItem should have children
        const yourAssetsItem = result.current?.items?.[1]?.children?.[0];
        yourAssetsItem.onClick?.({} as any); // simulate click

        expect(mockAddModule).toHaveBeenCalledWith({
            module: expect.objectContaining({
                urn: 'urn:li:dataHubPageModule:your_assets',
                properties: expect.objectContaining({
                    name: 'Your Assets',
                    type: DataHubPageModuleType.OwnedAssets,
                }),
            }),
            position: mockPosition,
        });
        expect(mockCloseMenu).toHaveBeenCalled();
    });

    it('should call addModule and closeMenu when Domains is clicked', () => {
        const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));

        // @ts-expect-error SubMenuItem should have children
        const domainsItem = result.current?.items?.[1]?.children?.[1];
        domainsItem.onClick?.({} as any); // simulate click

        expect(mockAddModule).toHaveBeenCalledWith({
            module: expect.objectContaining({
                urn: 'urn:li:dataHubPageModule:top_domains',
                properties: expect.objectContaining({
                    name: 'Domains',
                    type: DataHubPageModuleType.Domains,
                }),
            }),
            position: mockPosition,
        });
        expect(mockCloseMenu).toHaveBeenCalled();
    });

    it('should call addModule and closeMenu when admin created module is clicked', () => {
        // Mock the context to return globalTemplate with custom modules
        mockUsePageTemplateContext.mockReturnValue({
            addModule: mockAddModule,
            moduleModalState: {
                open: mockOpenModal,
                close: vi.fn(),
                isOpen: false,
                isEditing: false,
            },
            template: mockTemplate,
            globalTemplate: mockGlobalTemplate,
            templateType: PageTemplateSurfaceType.HomePage,
        });

        const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));

        // Navigate to admin module: items[2] -> children[0] (homeDefaults) -> children[0] (first admin module)
        // @ts-expect-error SubMenuItem should have children
        const homeDefaults = result.current?.items?.[2]?.children?.[0];
        const adminModuleItem = homeDefaults?.children?.[0];
        adminModuleItem.onClick?.({} as any); // simulate click

        expect(mockAddModule).toHaveBeenCalledWith({
            module: mockGlobalTemplate.properties.rows[0].modules[0],
            position: mockPosition,
        });
        expect(mockCloseMenu).toHaveBeenCalled();
    });

    it('should set expandIcon and popupClassName for admin created modules group', () => {
        // Mock the context to return globalTemplate with custom modules
        mockUsePageTemplateContext.mockReturnValue({
            addModule: mockAddModule,
            moduleModalState: {
                open: mockOpenModal,
                close: vi.fn(),
                isOpen: false,
                isEditing: false,
            },
            template: mockTemplate,
            globalTemplate: mockGlobalTemplate,
            templateType: PageTemplateSurfaceType.HomePage,
        });

        const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));

        // @ts-expect-error SubMenuItem should have children
        const homeDefaults = result.current?.items?.[2]?.children?.[0];
        expect(homeDefaults).toHaveProperty('expandIcon');
        expect(homeDefaults).toHaveProperty('popupClassName');
    });

    it('should open module modal when Asset Collection is clicked', () => {
        const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
        const customChildren = getChildren(result.current.items?.[0]);
        // Second child is Asset Collection (index 1)
        const assetCollectionItem = customChildren[1];
        assetCollectionItem.onClick?.({} as any);

        expect(mockOpenModal).toHaveBeenCalledWith(DataHubPageModuleType.AssetCollection, mockPosition);
        expect(mockCloseMenu).toHaveBeenCalled();
    });

    it('should not call addModule when Asset Collection is clicked', () => {
        const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
        const customChildren = getChildren(result.current.items?.[0]);
        const assetCollectionItem = customChildren[1];
        assetCollectionItem.onClick?.({} as any);

        expect(mockAddModule).not.toHaveBeenCalledWith(
            expect.objectContaining({
                module: expect.objectContaining({
                    urn: 'urn:li:dataHubPageModule:asset-collection',
                }),
            }),
            mockPosition,
        );
    });

    it('should not open modal when Your Assets is clicked', () => {
        const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
        const defaultChildren = getChildren(result.current.items?.[1]);
        const yourAssetsItem = defaultChildren[0];
        yourAssetsItem.onClick?.({} as any);

        expect(mockOpenModal).not.toHaveBeenCalled();
    });

    it('should open module modal when Quick Link is clicked', () => {
        const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
        const customChildren = getChildren(result.current.items?.[0]);
        const quickLinkItem = customChildren[0];
        quickLinkItem.onClick?.({} as any);

        expect(mockOpenModal).toHaveBeenCalledWith(DataHubPageModuleType.Link, mockPosition);
        expect(mockCloseMenu).toHaveBeenCalled();
    });

    it('should open module modal when Documentation is clicked', () => {
        const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
        const customChildren = getChildren(result.current.items?.[0]);
        const documentationItem = customChildren[2];
        documentationItem.onClick?.({} as any);

        expect(mockOpenModal).toHaveBeenCalledWith(DataHubPageModuleType.RichText, mockPosition);
        expect(mockCloseMenu).toHaveBeenCalled();
    });

    it('should open module modal when Hierarchy is clicked', () => {
        const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
        const customChildren = getChildren(result.current.items?.[0]);
        const hierarchyItem = customChildren[3];
        hierarchyItem.onClick?.({} as any);

        expect(mockOpenModal).toHaveBeenCalledWith(DataHubPageModuleType.Hierarchy, mockPosition);
        expect(mockCloseMenu).toHaveBeenCalled();
    });

    describe('TemplateType-based functionality', () => {
        it('should show different custom modules for AssetSummary vs HomePage', () => {
            // Test AssetSummary - should NOT have Quick Link
            mockUsePageTemplateContext.mockReturnValue({
                addModule: mockAddModule,
                moduleModalState: { open: mockOpenModal, close: vi.fn(), isOpen: false, isEditing: false },
                template: mockTemplate,
                globalTemplate: mockEmptyGlobalTemplate,
                templateType: PageTemplateSurfaceType.AssetSummary,
            });

            const { result: summaryResult } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
            const summaryCustomChildren = getChildren(summaryResult.current.items?.[0]);

            // AssetSummary should have 3 custom modules (no Quick Link)
            expect(summaryCustomChildren).toHaveLength(3);
            expect(summaryCustomChildren[0]).toHaveProperty('key', 'asset-collection');
            expect(summaryCustomChildren[1]).toHaveProperty('key', 'documentation');
            expect(summaryCustomChildren[2]).toHaveProperty('key', 'hierarchyView');

            // Test HomePage - should have Quick Link
            mockUsePageTemplateContext.mockReturnValue({
                addModule: mockAddModule,
                moduleModalState: { open: mockOpenModal, close: vi.fn(), isOpen: false, isEditing: false },
                template: mockTemplate,
                globalTemplate: mockEmptyGlobalTemplate,
                templateType: PageTemplateSurfaceType.HomePage,
            });

            const { result: homeResult } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
            const homeCustomChildren = getChildren(homeResult.current.items?.[0]);

            // HomePage should have 4 custom modules (including Quick Link)
            expect(homeCustomChildren).toHaveLength(4);
            expect(homeCustomChildren[0]).toHaveProperty('key', 'quick-link');
            expect(homeCustomChildren[1]).toHaveProperty('key', 'asset-collection');
            expect(homeCustomChildren[2]).toHaveProperty('key', 'documentation');
            expect(homeCustomChildren[3]).toHaveProperty('key', 'hierarchyView');
        });

        it('should only show Shared group for HomePage templates', () => {
            // Test AssetSummary - should NOT have Shared group
            mockUsePageTemplateContext.mockReturnValue({
                addModule: mockAddModule,
                moduleModalState: { open: mockOpenModal, close: vi.fn(), isOpen: false, isEditing: false },
                template: mockTemplate,
                globalTemplate: mockGlobalTemplate,
                templateType: PageTemplateSurfaceType.AssetSummary,
            });

            const { result: summaryResult } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
            expect(summaryResult.current.items).toHaveLength(2); // No Shared group

            // Test HomePage - should have Shared group
            mockUsePageTemplateContext.mockReturnValue({
                addModule: mockAddModule,
                moduleModalState: { open: mockOpenModal, close: vi.fn(), isOpen: false, isEditing: false },
                template: mockTemplate,
                globalTemplate: mockGlobalTemplate,
                templateType: PageTemplateSurfaceType.HomePage,
            });

            const { result: homeResult } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
            expect(homeResult.current.items).toHaveLength(3); // Includes Shared group
            expect(homeResult.current.items?.[2]).toHaveProperty('key', 'sharedModulesGroup');
        });
    });

    describe('EntityType-based functionality', () => {
        beforeEach(() => {
            // Set to AssetSummary so we test entity-specific default modules
            mockUsePageTemplateContext.mockReturnValue({
                addModule: mockAddModule,
                moduleModalState: { open: mockOpenModal, close: vi.fn(), isOpen: false, isEditing: false },
                template: mockTemplate,
                globalTemplate: mockEmptyGlobalTemplate,
                templateType: PageTemplateSurfaceType.AssetSummary,
            });
        });

        it('should show Domain-specific modules for Domain entity', () => {
            mockUseEntityData.mockReturnValue({ entityType: EntityType.Domain });

            const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
            const defaultChildren = getChildren(result.current.items?.[1]);

            // Domain should have: assets, childHierarchy, dataProducts
            expect(defaultChildren).toHaveLength(3);
            expect(defaultChildren[0]).toHaveProperty('key', 'assets');
            expect(defaultChildren[1]).toHaveProperty('key', 'hierarchy');
            expect(defaultChildren[2]).toHaveProperty('key', 'dataProducts');
        });

        it('should show GlossaryNode-specific modules for GlossaryNode entity', () => {
            mockUseEntityData.mockReturnValue({ entityType: EntityType.GlossaryNode });

            const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
            const defaultChildren = getChildren(result.current.items?.[1]);

            // GlossaryNode should have only: childHierarchy
            expect(defaultChildren).toHaveLength(1);
            expect(defaultChildren[0]).toHaveProperty('key', 'hierarchy');
        });

        it('should show GlossaryTerm-specific modules for GlossaryTerm entity', () => {
            mockUseEntityData.mockReturnValue({ entityType: EntityType.GlossaryTerm });

            const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
            const defaultChildren = getChildren(result.current.items?.[1]);

            // GlossaryTerm should have: assets, relatedTerms
            expect(defaultChildren).toHaveLength(2);
            expect(defaultChildren[0]).toHaveProperty('key', 'assets');
            expect(defaultChildren[1]).toHaveProperty('key', 'relatedTerms');
        });

        it('should show default modules for other entity types', () => {
            mockUseEntityData.mockReturnValue({ entityType: EntityType.Dataset });

            const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
            const defaultChildren = getChildren(result.current.items?.[1]);

            // Other entities should have only: assets
            expect(defaultChildren).toHaveLength(1);
            expect(defaultChildren[0]).toHaveProperty('key', 'assets');
        });
    });

    describe('New module functionality', () => {
        beforeEach(() => {
            mockUseEntityData.mockReturnValue({ entityType: EntityType.Domain });
            mockUsePageTemplateContext.mockReturnValue({
                addModule: mockAddModule,
                moduleModalState: { open: mockOpenModal, close: vi.fn(), isOpen: false, isEditing: false },
                template: mockTemplate,
                globalTemplate: mockEmptyGlobalTemplate,
                templateType: PageTemplateSurfaceType.AssetSummary,
            });
        });

        it('should call addModule with CHILD_HIERARCHY_MODULE when child hierarchy is clicked', () => {
            const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
            const defaultChildren = getChildren(result.current.items?.[1]);
            const childHierarchyItem = defaultChildren[1]; // Second item for Domain

            childHierarchyItem.onClick?.({} as any);

            expect(mockAddModule).toHaveBeenCalledWith({
                module: expect.objectContaining({
                    urn: 'urn:li:dataHubPageModule:child_hierarchy',
                    properties: expect.objectContaining({
                        name: 'Children',
                        type: DataHubPageModuleType.ChildHierarchy,
                    }),
                }),
                position: mockPosition,
            });
            expect(mockCloseMenu).toHaveBeenCalled();
        });

        it('should call addModule with DATA_PRODUCTS_MODULE when data products is clicked', () => {
            const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
            const defaultChildren = getChildren(result.current.items?.[1]);
            const dataProductsItem = defaultChildren[2]; // Third item for Domain

            dataProductsItem.onClick?.({} as any);

            expect(mockAddModule).toHaveBeenCalledWith({
                module: expect.objectContaining({
                    urn: 'urn:li:dataHubPageModule:data_products',
                    properties: expect.objectContaining({
                        name: 'Data Products',
                        type: DataHubPageModuleType.DataProducts,
                    }),
                }),
                position: mockPosition,
            });
            expect(mockCloseMenu).toHaveBeenCalled();
        });

        it('should call addModule with RELATED_TERMS_MODULE when related terms is clicked', () => {
            mockUseEntityData.mockReturnValue({ entityType: EntityType.GlossaryTerm });

            const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));
            const defaultChildren = getChildren(result.current.items?.[1]);
            const relatedTermsItem = defaultChildren[1]; // Second item for GlossaryTerm

            relatedTermsItem.onClick?.({} as any);

            expect(mockAddModule).toHaveBeenCalledWith({
                module: expect.objectContaining({
                    urn: 'urn:li:dataHubPageModule:related_terms',
                    properties: expect.objectContaining({
                        name: 'Related Terms',
                        type: DataHubPageModuleType.RelatedTerms,
                    }),
                }),
                position: mockPosition,
            });
            expect(mockCloseMenu).toHaveBeenCalled();
        });
    });

    describe('Combined templateType and entityType scenarios', () => {
        it('should show correct modules for HomePage with Domain entity', () => {
            mockUseEntityData.mockReturnValue({ entityType: EntityType.Domain });
            mockUsePageTemplateContext.mockReturnValue({
                addModule: mockAddModule,
                moduleModalState: { open: mockOpenModal, close: vi.fn(), isOpen: false, isEditing: false },
                template: mockTemplate,
                globalTemplate: mockEmptyGlobalTemplate,
                templateType: PageTemplateSurfaceType.HomePage,
            });

            const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));

            // Custom modules should have Quick Link (HomePage)
            const customChildren = getChildren(result.current.items?.[0]);
            expect(customChildren).toHaveLength(4);
            expect(customChildren[0]).toHaveProperty('key', 'quick-link');

            // Default modules should be HomePage defaults (not entity-specific)
            const defaultChildren = getChildren(result.current.items?.[1]);
            expect(defaultChildren).toHaveLength(3);
            expect(defaultChildren[0]).toHaveProperty('key', 'your-assets');
            expect(defaultChildren[1]).toHaveProperty('key', 'domains');
        });

        it('should show correct modules for AssetSummary with GlossaryNode entity', () => {
            mockUseEntityData.mockReturnValue({ entityType: EntityType.GlossaryNode });
            mockUsePageTemplateContext.mockReturnValue({
                addModule: mockAddModule,
                moduleModalState: { open: mockOpenModal, close: vi.fn(), isOpen: false, isEditing: false },
                template: mockTemplate,
                globalTemplate: mockEmptyGlobalTemplate,
                templateType: PageTemplateSurfaceType.AssetSummary,
            });

            const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));

            // Custom modules should NOT have Quick Link (AssetSummary)
            const customChildren = getChildren(result.current.items?.[0]);
            expect(customChildren).toHaveLength(3);
            expect(customChildren[0]).toHaveProperty('key', 'asset-collection');

            // Default modules should be GlossaryNode-specific
            const defaultChildren = getChildren(result.current.items?.[1]);
            expect(defaultChildren).toHaveLength(1);
            expect(defaultChildren[0]).toHaveProperty('key', 'hierarchy');
        });
    });
});
