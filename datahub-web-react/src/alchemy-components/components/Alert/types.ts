import React from 'react';

import { IconProps } from '@components/components/Icon/types';

/** Supported visual variants for the Alert component */
export type AlertVariant = 'success' | 'error' | 'warning' | 'info' | 'brand' | 'gray';

export interface AlertAction {
    label: string;
    onClick: () => void;
    icon?: IconProps;
    dataTestId?: string;
}

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
    /**
     * Optional action. Prefer `{ label, onClick }` — Alert renders a text button
     * whose color matches the alert variant (e.g. success → green), so callers
     * don't choose a button variant or color. ReactNode is still accepted for
     * existing call sites that pass a custom element.
     */
    action?: AlertAction | React.ReactNode;
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
