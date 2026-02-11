export interface BreadcrumbItem {
    label: string | React.ReactNode;
    href?: string;
    onClick?: () => void;
    isCurrent?: boolean;
    separator?: React.ReactNode;
}

export interface BreadcrumbProps {
    items: BreadcrumbItem[];
}
