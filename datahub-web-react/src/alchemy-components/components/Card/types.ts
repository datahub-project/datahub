import { IconAlignmentOptions } from '@src/alchemy-components/theme/config';

export type CardSize = 'sm' | 'md';

export interface CardProps {
    title?: string | React.ReactNode;
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
    dataTestId?: string;
    noOfSubtitleLines?: number;
    iconStyles?: React.CSSProperties;
    pillLabel?: string;
    pill?: React.ReactNode;
    /** Compact (`sm`) or default (`md`) card sizing */
    size?: CardSize;
    /** Enables accordion expand/collapse for the card body */
    collapsible?: boolean;
    /** Initial expanded state when uncontrolled. Defaults to `true`. */
    defaultExpanded?: boolean;
    /** Controlled expanded state */
    expanded?: boolean;
    /** Called when the expanded state changes */
    onExpandChange?: (expanded: boolean) => void;
}
