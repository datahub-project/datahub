import { useMemo } from 'react';
import { matchPath, useLocation } from 'react-router';
import { NavBarMenuItems, NavBarMenuItemTypes } from './types';

export default function useSelectedKey(menu: NavBarMenuItems) {
    const location = useLocation();

    return useMemo(() => {
        const allItems = menu.items.flatMap((item) => {
            if (item.type === NavBarMenuItemTypes.Group && item.items) {
                return [item, ...item.items];
            }
            return [item];
        });

        const matchedItems = allItems
            .map((currentItem) => {
                const match = matchPath(location.pathname, currentItem.link ? [currentItem.link] : []);
                return {
                    ...currentItem,
                    path: match?.path,
                    exact: match?.isExact,
                };
            })
            .filter((currentItem) => {
                const isPathMatched = Boolean(currentItem.path);
                const isExactMatch = !currentItem.onlyExactPathMapping || currentItem.exact;
                return isPathMatched && isExactMatch;
            })
            .sort((a, b) => (b.path?.length ?? 0) - (a.path?.length ?? 0));

        return matchedItems[0]?.key;
    }, [location.pathname, menu]);
}
