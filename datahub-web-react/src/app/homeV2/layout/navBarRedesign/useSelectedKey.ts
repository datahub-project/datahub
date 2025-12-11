/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';
import { matchPath, useLocation } from 'react-router';

import { NavBarMenuItemTypes, NavBarMenuItems } from '@app/homeV2/layout/navBarRedesign/types';

export default function useSelectedKey(menu: NavBarMenuItems) {
    const location = useLocation();

    return useMemo(() => {
        const createItem = (item) => ({
            item,
            key: item.key,
            links: [item.link, ...(item.additionalLinksForPathMatching || [])],
        });

        const allItems = menu.items.flatMap((item) => {
            const baseItem = createItem(item);
            if (item.type === NavBarMenuItemTypes.Group && item.items) {
                const subItems = item.items.map(createItem);
                return [baseItem, ...subItems];
            }
            return [baseItem];
        });

        const matchedItems = allItems
            .map((currentItem) => {
                const match = matchPath(location.pathname, currentItem.links);
                return {
                    ...currentItem,
                    path: match?.path,
                    exact: match?.isExact,
                };
            })
            .filter((currentItem) => {
                const isPathMatched = Boolean(currentItem.path);
                const isExactMatch = !currentItem.item.onlyExactPathMapping || currentItem.exact;
                return isPathMatched && isExactMatch;
            })
            .sort((a, b) => (b.path?.length ?? 0) - (a.path?.length ?? 0));

        return matchedItems[0]?.key;
    }, [location.pathname, menu]);
}
