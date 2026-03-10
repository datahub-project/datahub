import React from 'react';

/** Supported visual variants for the Alert component */
export type AlertVariant = 'success' | 'error' | 'warning' | 'info' | 'brand';

export interface AlertProps {
    /** Visual style variant determining colors and default icon */
    variant: AlertVariant;
    /** Primary message */
    title: string | React.ReactNode;
    /** Optional secondary message */
    description?: string | React.ReactNode;
    /** Override the default icon for the variant */
    icon?: React.ReactNode;
    /** Show a close/dismiss button */
    onClose?: () => void;
    /** Additional action element rendered on the right */
    action?: React.ReactNode;
    /** Additional CSS class */
    className?: string;
    /** Inline styles */
    style?: React.CSSProperties;
}
