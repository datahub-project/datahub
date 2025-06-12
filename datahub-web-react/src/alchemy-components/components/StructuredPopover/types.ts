export interface SectionType {
    title: string | React.ReactNode;
    titleSuffix?: string | React.ReactNode;
    content: string | React.ReactNode;
}

export interface StructuredPopoverProps {
    header?: React.ComponentType;
    sections?: SectionType[];
    children?: React.ReactNode;
    width?: number;
}

export interface TooltipHeaderProps {
    title: string;
    titleSuffix?: React.ReactNode;
    subTitle?: string;
    image?: string;
    action?: React.ComponentType;
}
