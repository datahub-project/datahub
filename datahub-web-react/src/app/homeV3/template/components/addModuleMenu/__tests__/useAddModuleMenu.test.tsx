import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { vi } from 'vitest';

import type { ModuleInfo, ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import useAddModuleMenu from '@app/homeV3/template/components/addModuleMenu/useAddModuleMenu';
import { AddModuleHandlerInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

// Mock the PageTemplateContext
const mockAddModule = vi.fn();
vi.mock('@app/homeV3/context/PageTemplateContext', () => ({
    usePageTemplateContext: () => ({
        addModule: mockAddModule,
    }),
}));

// Mock components that are rendered inside the menu items
vi.mock('@app/homeV3/template/components/addModuleMenu/components/GroupItem', () => ({
    __esModule: true,
    default: ({ title }: { title: string }) => <div data-testid="group-item">{title}</div>,
}));

vi.mock('@app/homeV3/template/components/addModuleMenu/components/ModuleMenuItem', () => ({
    __esModule: true,
    default: ({ module }: { module: ModuleInfo }) => <div data-testid={`module-${module.key}`}>{module.name}</div>,
}));

vi.mock('@app/homeV3/template/components/addModuleMenu/components/MenuItem', () => ({
    __esModule: true,
    default: ({ title }: { title: string }) => <div data-testid="menu-item">{title}</div>,
}));

describe('useAddModuleMenu', () => {
    const mockCloseMenu = vi.fn();
    const mockPosition: AddModuleHandlerInput = { rowIndex: 0, rowSide: 'left' };

    const modulesAvailableToAdd: ModulesAvailableToAdd = {
        customModules: [], // Not used in new implementation
        customLargeModules: [], // Not used in new implementation
        adminCreatedModules: [
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
        ],
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return menu items with hardcoded custom modules', () => {
        const { result } = renderHook(() =>
            useAddModuleMenu(
                {
                    customModules: [],
                    customLargeModules: [],
                    adminCreatedModules: [],
                },
                mockPosition,
                mockCloseMenu,
            ),
        );

        const { items } = result.current;
        expect(items).toHaveLength(2);

        // Check Custom group
        expect(items?.[0]).toHaveProperty('key', 'customModulesGroup');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[0]?.children).toHaveLength(2);
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[0]?.children?.[0]).toHaveProperty('key', 'quick-link');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[0]?.children?.[1]).toHaveProperty('key', 'documentation');

        // Check Custom Large group
        expect(items?.[1]).toHaveProperty('key', 'customLargeModulesGroup');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[1]?.children).toHaveLength(2);
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[1]?.children?.[0]).toHaveProperty('key', 'your-assets');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[1]?.children?.[1]).toHaveProperty('key', 'domains');
    });

    it('should include admin created modules when available', () => {
        const { result } = renderHook(() => useAddModuleMenu(modulesAvailableToAdd, mockPosition, mockCloseMenu));

        const { items } = result.current;
        expect(items).toHaveLength(3);

        // Check Admin Created Widgets group
        expect(items?.[2]).toHaveProperty('key', 'adminCreatedModulesGroup');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[2]?.children).toHaveLength(1);
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[2]?.children?.[0]).toHaveProperty('key', 'urn:li:dataHubPageModule:admin1');
    });

    it('should call addModule and closeMenu when Your Assets is clicked', () => {
        const { result } = renderHook(() => useAddModuleMenu(modulesAvailableToAdd, mockPosition, mockCloseMenu));

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
        const { result } = renderHook(() => useAddModuleMenu(modulesAvailableToAdd, mockPosition, mockCloseMenu));

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
        const { result } = renderHook(() => useAddModuleMenu(modulesAvailableToAdd, mockPosition, mockCloseMenu));

        // @ts-expect-error SubMenuItem should have children
        const adminModuleItem = result.current?.items?.[2]?.children?.[0];
        adminModuleItem.onClick?.({} as any); // simulate click

        expect(mockAddModule).toHaveBeenCalledWith({
            module: modulesAvailableToAdd.adminCreatedModules[0],
            position: mockPosition,
        });
        expect(mockCloseMenu).toHaveBeenCalled();
    });

    it('should set expandIcon and popupClassName for admin created modules group', () => {
        const { result } = renderHook(() => useAddModuleMenu(modulesAvailableToAdd, mockPosition, mockCloseMenu));

        const adminGroup = result.current?.items?.[2];
        expect(adminGroup).toHaveProperty('expandIcon');
        expect(adminGroup).toHaveProperty('popupClassName');
    });
});
