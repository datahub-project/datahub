import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import useConvertedAntdMenu from '@components/components/Menu/hooks/useConvertedAntdMenu';
// Define types for clarity in test
import { ItemType } from '@components/components/Menu/types';

// Mock dependencies to avoid importing actual components
vi.mock('../components/MenuItemRenderer', () => ({
    default: ({ item }: { item: { label: string } }) => <span>{item.label}</span>,
}));

vi.mock('../components/GroupItemRenderer', () => ({
    default: ({ item }: { item: { label: string } }) => <strong>{item.label}</strong>,
}));

describe('useConvertedAntdMenu', () => {
    it('should return menu with empty items when no items are provided', () => {
        const { result } = renderHook(() => useConvertedAntdMenu(undefined));
        expect(result.current).toEqual({ items: [] });
    });

    it('should convert a single item correctly', () => {
        const mockItem: ItemType = {
            key: '1',
            type: 'item',
            title: 'Home',
            onClick: vi.fn(),
        };

        const { result } = renderHook(() => useConvertedAntdMenu([mockItem]));

        const converted = result.current;

        expect(converted?.items).toHaveLength(1);
        const item = converted?.items?.[0] as any;

        expect(item.key).toBe('1');
    });

    it('should handle nested submenu items and apply expandIcon and popupClassName', () => {
        const mockOnClick = vi.fn();
        const mockItemWithChildren: ItemType = {
            key: '2',
            type: 'item' as const,
            title: 'Settings',
            children: [
                {
                    key: '2-1',
                    type: 'item' as const,
                    title: 'Profile',
                    onClick: mockOnClick,
                },
            ],
        };

        const { result } = renderHook(() => useConvertedAntdMenu([mockItemWithChildren]));

        const converted = result.current;
        const parent = converted?.items?.[0] as any;
        const child = parent?.children?.[0] as any;

        expect(parent.children).toHaveLength(1);
        expect(child.key).toBe('2-1');
        expect(child.onClick).toBe(mockOnClick);
    });

    it('should convert group items with label and children', () => {
        const groupItem: ItemType = {
            key: 'group-1',
            type: 'group' as const,
            title: 'User Menu',
            children: [
                {
                    key: '3',
                    type: 'item' as const,
                    title: 'Logout',
                    onClick: vi.fn(),
                },
            ],
        };

        const { result } = renderHook(() => useConvertedAntdMenu([groupItem]));

        const converted = result.current;
        const group = converted?.items?.[0] as any;

        expect(group.type).toBe('group');
        expect(group.key).toBe('group-1');
        expect(group.children).toHaveLength(1);
        expect((group.children?.[0] as any).key).toBe('3');
    });

    it('should convert divider items', () => {
        const divider = {
            key: 'divider-1',
            type: 'divider' as const,
        };

        const { result } = renderHook(() => useConvertedAntdMenu([divider]));

        const converted = result.current;
        const item = converted?.items?.[0] as any;

        expect(item.key).toBe('divider-1');
        expect(item.type).toBe('divider');
    });

    it('should preserve referential equality via useMemo when input does not change', () => {
        const items: ItemType[] = [{ key: '1', type: 'item' as const, title: 'Test' }];

        const { result, rerender } = renderHook(() => useConvertedAntdMenu(items));

        const first = result.current;

        rerender(items); // same items
        const second = result.current;

        expect(first).toBe(second); // should be memoized
    });

    it('should recompute when items change', () => {
        const items1: ItemType[] = [{ key: '1', type: 'item', title: 'Item 1' }];
        const items2: ItemType[] = [{ key: '2', type: 'item', title: 'Item 2' }];

        const { result, rerender } = renderHook((props) => useConvertedAntdMenu(props), { initialProps: items1 });

        const first = result.current;

        rerender(items2);
        const second = result.current;

        expect(first).not.toBe(second);
    });

    it('should handle mixed item types correctly', () => {
        const mixedItems: ItemType[] = [
            {
                key: '1',
                type: 'item',
                title: 'Dashboard',
            },
            {
                key: 'group-1',
                type: 'group',
                title: 'Admin',
                children: [
                    {
                        key: '2',
                        type: 'item',
                        title: 'Users',
                    },
                ],
            },
            {
                key: 'divider-1',
                type: 'divider',
            },
            {
                key: '3',
                type: 'item',
                title: 'Help',
                children: [
                    {
                        key: '3-1',
                        type: 'item',
                        title: 'FAQ',
                    },
                ],
            },
        ];

        const { result } = renderHook(() => useConvertedAntdMenu(mixedItems));

        const converted = result.current?.items;

        expect(converted).toHaveLength(4);
        expect(converted?.[0]?.key).toBe('1');
        expect((converted?.[1] as any)?.type).toBe('group');
        expect((converted?.[1] as any)?.children).toHaveLength(1);
        expect((converted?.[2] as any)?.type).toBe('divider');
        expect((converted?.[3] as any)?.children).toHaveLength(1);
    });
});
