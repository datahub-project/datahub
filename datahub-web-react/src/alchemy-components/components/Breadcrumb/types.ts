export interface BreadcrumbItemType {
    key: string;
    label: string | React.ReactNode;
    href?: string;
    onClick?: () => void;
    isActive?: boolean;
    separator?: React.ReactNode;
}

export interface BreadcrumbProps {
    items: BreadcrumbItemType[];
    showPopover?: boolean;
}
