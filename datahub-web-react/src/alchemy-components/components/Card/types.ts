import { IconAlignmentOptions } from '@src/alchemy-components/theme/config';

export interface CardProps {
<<<<<<< HEAD
    title: React.ReactNode;
    subTitle?: React.ReactNode;
=======
    title: string | React.ReactNode;
    subTitle?: string | React.ReactNode;
>>>>>>> 3ab354eac4
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
