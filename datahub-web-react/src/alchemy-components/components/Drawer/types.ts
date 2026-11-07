export type DrawerPlacement = 'top' | 'right' | 'bottom' | 'left';

export type DrawerGetContainer = HTMLElement | (() => HTMLElement) | string | false;

export type DrawerProps = {
    /** Text or node shown in the default title bar. Ignored when `header` is set. */
    title?: React.ReactNode;
    /** Custom header rendered in place of the default title bar (title, back and close buttons). */
    header?: React.ReactNode;
    /** Set to false to render no header at all, e.g. when the content renders its own header. */
    showHeader?: boolean;
    /** Content rendered in a footer pinned below the scrollable body. */
    footer?: React.ReactNode;
    open?: boolean;
    onClose?: () => void;
    onBack?: () => void;
    width?: number | string;
    /** Minimum width of the drawer panel. */
    minWidth?: number | string;
    /** Maximum width of the drawer panel. */
    maxWidth?: number | string;
    /** Whether the default title bar shows a close (x) button. */
    closable?: boolean;
    maskTransparent?: boolean;
    /** Whether to render the mask behind the drawer. */
    mask?: boolean;
    /** Whether clicking the mask closes the drawer. */
    maskClosable?: boolean;
    placement?: DrawerPlacement;
    /** Where to mount the drawer. Pass an element (or a selector / getter) to render it inline inside that element. */
    getContainer?: DrawerGetContainer;
    /** Whether a parent drawer is pushed aside when this drawer opens nested inside it. */
    push?: boolean;
    /** Whether the drawer takes focus when it opens. */
    autoFocus?: boolean;
    /** Padding of the drawer body. Numbers are treated as pixels. */
    bodyPadding?: number | string;
    className?: string;
    dataTestId?: string;
};
