import { DrawerProps } from './types';

export const drawerDefault: Omit<DrawerProps, 'title'> = {
    width: 600,
    closable: true,
    maskTransparent: false,
};
