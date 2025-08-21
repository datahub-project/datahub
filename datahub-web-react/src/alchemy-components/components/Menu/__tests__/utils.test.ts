import { describe, it, expect } from 'vitest';
import { MenuItemType } from '@components/components/Menu/types';
import { sortMenuItems } from '@components/components/Menu/utils';

describe('sortMenuItems', () => {
    const unsortedItems: MenuItemType[] = [
        { type: 'item', key: 'c', title: 'Charlie' },
        { type: 'item', key: 'a', title: 'Alpha' },
        { type: 'item', key: 'b', title: 'Bravo' },
    ];

    it('should sort menu items alphabetically by title', () => {
        const sortedItems = sortMenuItems(unsortedItems);
        expect(sortedItems[0].title).toBe('Alpha');
        expect(sortedItems[1].title).toBe('Bravo');
        expect(sortedItems[2].title).toBe('Charlie');
    });

    it('should not mutate the original array', () => {
        sortMenuItems(unsortedItems);
        expect(unsortedItems[0].title).toBe('Charlie');
    });
});
