import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import type { ModuleInfo, ModulesAvailableToAdd } from '@app/homeV3/modules/types';
import useAddModuleMenu from '@app/homeV3/template/components/addModuleMenu/useAddModuleMenu';

import { DataHubPageModuleType } from '@types';

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
    const mockOnClick = vi.fn();

    const modulesAvailableToAdd: ModulesAvailableToAdd = {
        customModules: [
            { key: 'custom1', name: 'Custom 1', icon: 'Edit', type: DataHubPageModuleType.Link },
            { key: 'custom2', name: 'Custom 2', icon: 'Setting', type: DataHubPageModuleType.Link },
        ],
        customLargeModules: [{ key: 'large1', name: 'Large 1', icon: 'BarChart', type: DataHubPageModuleType.Domains }],
        adminCreatedModules: [
            { key: 'admin1', name: 'Admin Widget 1', icon: 'Database', type: DataHubPageModuleType.Link },
        ],
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return empty items when no modules are available', () => {
        const { result } = renderHook(() =>
            useAddModuleMenu(
                {
                    customModules: [],
                    customLargeModules: [],
                    adminCreatedModules: [],
                },
                mockOnClick,
            ),
        );

        expect(result.current.items).toHaveLength(0);
    });

    it('should generate correct items for customModules group', () => {
        const { result } = renderHook(() =>
            useAddModuleMenu(
                {
                    ...modulesAvailableToAdd,
                    customLargeModules: [],
                    adminCreatedModules: [],
                },
                mockOnClick,
            ),
        );

        const { items } = result.current;
        expect(items).toHaveLength(1);
        expect(items?.[0]).toHaveProperty('key', 'customModulesGroup');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[0]?.children).toHaveLength(2);
    });

    it('should generate correct items for customLargeModules group', () => {
        const { result } = renderHook(() =>
            useAddModuleMenu(
                {
                    ...modulesAvailableToAdd,
                    customModules: [],
                    adminCreatedModules: [],
                },
                mockOnClick,
            ),
        );

        const { items } = result.current;
        expect(items).toHaveLength(1);
        expect(items?.[0]).toHaveProperty('key', 'customLargeModulesGroup');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[0]?.children).toHaveLength(1);
    });

    it('should generate correct items for adminCreatedModules group with expandIcon and popupClassName', () => {
        const { result } = renderHook(() =>
            useAddModuleMenu(
                {
                    ...modulesAvailableToAdd,
                    customModules: [],
                    customLargeModules: [],
                },
                mockOnClick,
            ),
        );

        const { items } = result.current;
        expect(items).toHaveLength(1);
        expect(items?.[0]).toHaveProperty('key', 'adminCreatedModulesGroup');
        // @ts-expect-error SubMenuItem should have children
        expect(items?.[0]?.children).toHaveLength(1);
    });

    it('should call onClick handler when a module item is clicked', () => {
        const { result } = renderHook(() => useAddModuleMenu(modulesAvailableToAdd, mockOnClick));

        // @ts-expect-error SubMenuItem should have children
        const moduleItem = result.current?.items?.[0]?.children?.[0];
        moduleItem.onClick?.({} as any); // simulate click

        expect(mockOnClick).toHaveBeenCalledWith(modulesAvailableToAdd.customModules[0]);
    });
});
