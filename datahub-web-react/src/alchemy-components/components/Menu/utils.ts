import { MenuItemType } from '@components/components/Menu/types';

export function sortMenuItems(items: MenuItemType[]): MenuItemType[] {
    return [...items].sort((a, b) => a.title.localeCompare(b.title));
}
