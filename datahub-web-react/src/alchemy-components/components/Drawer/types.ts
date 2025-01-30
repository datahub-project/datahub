export type DrawerProps = {
    title: React.ReactNode;
    open?: boolean;
    onClose?: () => void;
    width?: number | string;
    closable?: boolean;
    maskTransparent?: boolean;
};
