import { useMemo } from 'react';
import { matchPath, useLocation } from 'react-router';
import { NavBarMenuItems, NavBarMenuItemTypes } from './types';

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
