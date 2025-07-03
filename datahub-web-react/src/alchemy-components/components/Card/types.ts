import { IconAlignmentOptions } from '@src/alchemy-components/theme/config';

export interface CardProps {
    title: string | React.ReactNode;
    subTitle?: string | React.ReactNode;
    percent?: number;
    button?: React.ReactNode;
    onClick?: () => void;
    icon?: React.ReactNode;
    iconAlignment?: IconAlignmentOptions;
    children?: React.ReactNode;
    width?: string;
    maxWidth?: string;
    height?: string;
    isEmpty?: boolean;
    style?: React.CSSProperties;
    isCardClickable?: boolean;
}
