export interface IconLabelProps {
    icon: JSX.Element;
    name: string;
    type: IconType;
    marginRight?: string;
    imageUrl?: string;
    style?: React.CSSProperties;
}

export enum IconType {
    ICON = 'ICON',
    IMAGE = 'IMAGE',
}
