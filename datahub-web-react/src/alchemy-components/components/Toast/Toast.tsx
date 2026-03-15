import { CheckCircle, CircleNotch, Info, Warning, WarningCircle, X } from '@phosphor-icons/react';
import React, { useEffect, useState } from 'react';
import ReactDOM from 'react-dom';
import { ThemeProvider, useTheme } from 'styled-components';

import {
    ANIMATION_DURATION_MS,
    ToastActionButton,
    ToastBody,
    ToastCloseButton,
    ToastContainer,
    ToastWrapper,
} from '@components/components/Toast/components';
import { ToastAPI, ToastOptions, ToastVariant } from '@components/components/Toast/types';

const MAX_TOASTS = 5;

const DEFAULT_DURATIONS: Record<ToastVariant, number> = {
    success: 3,
    error: 4,
    info: 3,
    warning: 4,
    loading: 0,
};

const VARIANT_ICONS: Record<ToastVariant, React.ReactNode> = {
    success: <CheckCircle size={18} weight="fill" />,
    error: <WarningCircle size={18} weight="fill" />,
    warning: <Warning size={18} weight="fill" />,
    info: <Info size={18} weight="fill" />,
    loading: <CircleNotch size={18} weight="bold" />,
};

const ASSERTIVE_VARIANTS: ReadonlySet<ToastVariant> = new Set(['error', 'warning']);

interface ToastEntry {
    id: string;
    variant: ToastVariant;
    content: React.ReactNode;
    options?: ToastOptions;
    exiting?: boolean;
}

type Listener = () => void;

let toasts: ToastEntry[] = [];
let listeners: Listener[] = [];
const timers = new Map<string, ReturnType<typeof setTimeout>>();

function emit() {
    listeners.forEach((l) => l());
}

function subscribe(listener: Listener) {
    listeners.push(listener);
    return () => {
        listeners = listeners.filter((l) => l !== listener);
    };
}

function removeToast(id: string) {
    const target = toasts.find((t) => t.id === id);
    if (!target || target.exiting) return;

    toasts = toasts.map((t) => (t.id === id ? { ...t, exiting: true } : t));
    emit();

    setTimeout(() => {
        const entry = toasts.find((t) => t.id === id);
        if (entry?.options?.onClose) entry.options.onClose();
        toasts = toasts.filter((t) => t.id !== id);
        timers.delete(id);
        emit();
    }, ANIMATION_DURATION_MS);
}

function addToast(variant: ToastVariant, content: string | React.ReactNode, options?: ToastOptions) {
    const id = options?.key || `toast-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
    const duration = options?.duration !== undefined ? options.duration : DEFAULT_DURATIONS[variant];

    const existing = toasts.find((t) => t.id === id);
    if (existing) {
        toasts = toasts.map((t) => (t.id === id ? { ...t, variant, content, options, exiting: false } : t));
        const prevTimer = timers.get(id);
        if (prevTimer) clearTimeout(prevTimer);
    } else {
        toasts = [...toasts, { id, variant, content, options }];
        // Evict oldest when over the limit
        while (toasts.length > MAX_TOASTS) {
            const oldest = toasts[0];
            toasts = toasts.slice(1);
            const oldTimer = timers.get(oldest.id);
            if (oldTimer) clearTimeout(oldTimer);
            timers.delete(oldest.id);
        }
    }

    emit();

    if (duration && duration > 0) {
        const timer = setTimeout(() => removeToast(id), duration * 1000);
        timers.set(id, timer);
    }
}

function destroyToast(key?: string) {
    if (key) {
        removeToast(key);
    } else {
        timers.forEach((timer) => clearTimeout(timer));
        timers.clear();
        const pending = toasts;
        toasts = [];
        emit();
        pending.forEach((entry) => entry.options?.onClose?.());
    }
}

const ToastItem = React.memo(({ entry }: { entry: ToastEntry }) => {
    const icon = entry.options?.icon ?? VARIANT_ICONS[entry.variant];
    const liveRegion = ASSERTIVE_VARIANTS.has(entry.variant) ? 'assertive' : 'polite';

    return (
        <ToastWrapper $variant={entry.variant} $exiting={entry.exiting} role="status" aria-live={liveRegion}>
            <ToastBody>
                <span className="toast-icon">{icon}</span>
                <span className="toast-text">{entry.content}</span>
            </ToastBody>
            {entry.options?.actions?.map((action) => (
                <ToastActionButton
                    key={action.label}
                    className="toast-btn"
                    type="button"
                    onClick={action.onClick}
                    aria-label={action.label}
                    title={action.label}
                >
                    {action.icon ?? action.label}
                </ToastActionButton>
            ))}
            <ToastCloseButton
                className="toast-btn"
                type="button"
                onClick={() => removeToast(entry.id)}
                aria-label="Dismiss"
            >
                <X size={14} weight="bold" />
            </ToastCloseButton>
        </ToastWrapper>
    );
});

/**
 * Mount once inside the app's ThemeProvider tree.
 * Renders toast notifications into a portal at the top-right of the viewport.
 */
export function ToastRenderer() {
    const [, setTick] = useState(0);
    const theme = useTheme();

    useEffect(() => {
        return subscribe(() => setTick((t) => t + 1));
    }, []);

    const currentToasts = toasts;
    if (currentToasts.length === 0) return null;

    return ReactDOM.createPortal(
        <ThemeProvider theme={theme}>
            <ToastContainer>
                {currentToasts.map((entry) => (
                    <ToastItem key={entry.id} entry={entry} />
                ))}
            </ToastContainer>
        </ThemeProvider>,
        document.body,
    );
}

export const toast: ToastAPI = {
    success: (content, options) => addToast('success', content, options),
    error: (content, options) => addToast('error', content, options),
    info: (content, options) => addToast('info', content, options),
    warning: (content, options) => addToast('warning', content, options),
    loading: (content, options) => addToast('loading', content, options),
    destroy: destroyToast,
};
