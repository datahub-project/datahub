import React from 'react';

import { IconProps } from '@components/components/Icon/types';

export type EmptyStateSize = 'sm' | 'default' | 'lg';

export interface EmptyStateAction {
    label: string;
    onClick: () => void;
    icon?: IconProps;
    variant?: 'filled' | 'secondary' | 'text';
}

export interface EmptyStateProps {
    /** Primary heading text */
    title: string;
    /** Optional secondary description text */
    description?: string | React.ReactNode;
    /** Phosphor icon name (e.g. "MagnifyingGlass", "Key", "Robot") */
    icon?: string;
    /** Override the default icon with a custom React node (e.g. an image or SVG) */
    image?: React.ReactNode;
    /** Primary action button */
    action?: EmptyStateAction;
    /** Optional secondary action button */
    secondaryAction?: EmptyStateAction;
    /** Size variant controlling icon size and spacing */
    size?: EmptyStateSize;
    /** Additional CSS class */
    className?: string;
    /** Inline styles */
    style?: React.CSSProperties;
}
