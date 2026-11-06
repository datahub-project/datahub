import { DrawerProps } from '@components/components/Drawer/types';

export const drawerDefault: Omit<DrawerProps, 'title'> = {
    width: 600,
    closable: true,
    maskTransparent: false,
    showHeader: true,
    mask: true,
    maskClosable: true,
    placement: 'right',
    bodyPadding: 16,
};
