import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { vi } from 'vitest';

import useAddModuleMenu from '@app/homeV3/template/components/addModuleMenu/useAddModuleMenu';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

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

vi.mock('@app/homeV3/context/PageTemplateContext', () => ({
    usePageTemplateContext: mockUsePageTemplateContext,
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
        });
    });

    function getChildren(item: any): any[] {
        if (item && 'children' in item && Array.isArray(item.children)) return item.children;
        return [];
    }

    it('should return menu items with correct structure when no global template custom modules exist', () => {
        const { result } = renderHook(() => useAddModuleMenu(mockPosition, mockCloseMenu));

        const { items } = result.current;
        expect(items).toHaveLength(2);

        // Check "Create Your Own" group
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

        // Check "Default by DataHub" group
        expect(items?.[1]).toHaveProperty('key', 'customLargeModulesGroup');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[1]?.children).toHaveLength(2);
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[1]?.children?.[0]).toHaveProperty('key', 'your-assets');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[1]?.children?.[1]).toHaveProperty('key', 'domains');
    });

    it('should include admin created modules when available in global template', () => {
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
});
