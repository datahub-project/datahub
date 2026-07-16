import React from 'react';

export type ToastVariant = 'success' | 'error' | 'info' | 'warning' | 'loading';

export interface ToastAction {
    label: string;
    onClick: () => void;
    /** Pass a React element (e.g. <ArrowCounterClockwise size={16} weight="bold" />) */
    icon?: React.ReactNode;
}

export interface ToastOptions {
    /** Duration in seconds. 0 = persistent. Defaults vary by variant. */
    duration?: number;
    /** Optional action buttons (retry, undo, etc.) */
    actions?: ToastAction[];
    /** Unique key to update/replace an existing toast */
    key?: string;
    /** Override the default icon */
    icon?: React.ReactNode;
    /** Callback when toast closes */
    onClose?: () => void;
}

export interface ToastAPI {
    success: (content: string | React.ReactNode, options?: ToastOptions) => void;
    error: (content: string | React.ReactNode, options?: ToastOptions) => void;
    info: (content: string | React.ReactNode, options?: ToastOptions) => void;
    warning: (content: string | React.ReactNode, options?: ToastOptions) => void;
    loading: (content: string | React.ReactNode, options?: ToastOptions) => void;
    destroy: (key?: string) => void;
}
