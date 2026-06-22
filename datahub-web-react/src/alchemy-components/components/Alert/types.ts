import React from 'react';

/** Supported visual variants for the Alert component */
export type AlertVariant = 'success' | 'error' | 'warning' | 'info' | 'brand' | 'unknown';

export interface AlertProps {
    /** Visual style variant determining colors and default icon */
    variant: AlertVariant;
    /** Primary message */
    title: string | React.ReactNode;
    /** Optional secondary message */
    description?: string | React.ReactNode;
    /** Technical error detail rendered in a monospace box below the description */
    errorMessage?: string;
    /** Override the default icon for the variant */
    icon?: React.ReactNode;
    /** Show a close/dismiss button */
    onClose?: () => void;
    /** Additional action element rendered on the right */
    action?: React.ReactNode;
    /**
     * Where to render the `action` element.
     * - `'inline'` (default): under the description, inside the content column.
     * - `'topRight'`: in the top-right corner, next to the close button.
     */
    actionPlacement?: 'inline' | 'topRight';
    /** Additional CSS class */
    className?: string;
    /** Inline styles */
    style?: React.CSSProperties;
    /** Test selector */
    'data-testid'?: string;
}
