/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { describe, expect, it } from 'vitest';

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
