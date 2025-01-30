export interface NavMenuItem {
    icon?: React.FunctionComponent<React.SVGProps<SVGSVGElement> & { title?: string }>;
    title: string;
    showNewTag?: boolean;
    description: string;
    link?: string | null;
    subMenu?: NavSubMenu;
    isHidden?: boolean;
    target?: string;
    rel?: string;
    onClick?: () => void;
}

export interface NavSubMenuItem {
    title: string;
    showNewTag?: boolean;
    description: string;
    link?: string | null;
    isHidden?: boolean;
    target?: string;
    rel?: string;
    onClick?: () => void;
}

export interface NavSubMenu {
    isOpen: boolean;
    open: () => void;
    close: () => void;
    items: NavSubMenuItem[];
}
