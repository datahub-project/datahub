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
