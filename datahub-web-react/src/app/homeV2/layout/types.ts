export interface NavMenuItem {
    icon?: React.FunctionComponent<React.SVGProps<SVGSVGElement> & { title?: string }>;
    title: string;
    description: string;
    link: string | null;
    subMenu?: NavSubMenu;
    isHidden?: boolean;
    target?: string;
    rel?: string;
}

export interface NavSubMenuItem {
    title: string;
    description: string;
    link: string | null;
    isHidden?: boolean;
    target?: string;
    rel?: string;
}

export interface NavSubMenu {
    isOpen: boolean;
    open: () => void;
    close: () => void;
    items: NavSubMenuItem[];
}