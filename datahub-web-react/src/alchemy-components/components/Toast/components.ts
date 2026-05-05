import styled, { DefaultTheme, css, keyframes } from 'styled-components';

import { ToastVariant } from '@components/components/Toast/types';
import { radius, spacing, typography, zIndices } from '@components/theme';

export const ANIMATION_DURATION_MS = 250;

const spin = keyframes`
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
`;

const slideIn = keyframes`
    from {
        transform: translateX(100%);
        opacity: 0;
    }
    to {
        transform: translateX(0);
        opacity: 1;
    }
`;

const slideOut = keyframes`
    from {
        transform: translateX(0);
        opacity: 1;
    }
    to {
        transform: translateX(100%);
        opacity: 0;
    }
`;

function getVariantColors(theme: DefaultTheme, variant: ToastVariant) {
    const map: Record<ToastVariant, { bg: string; bgHover: string; icon: string; text: string }> = {
        success: {
            bg: theme.colors.bgSurfaceSuccess,
            bgHover: theme.colors.bgSurfaceSuccessHover,
            icon: theme.colors.iconSuccess,
            text: theme.colors.textSuccess,
        },
        error: {
            bg: theme.colors.bgSurfaceError,
            bgHover: theme.colors.bgSurfaceErrorHover,
            icon: theme.colors.iconError,
            text: theme.colors.textError,
        },
        warning: {
            bg: theme.colors.bgSurfaceWarning,
            bgHover: theme.colors.bgSurfaceWarningHover,
            icon: theme.colors.iconWarning,
            text: theme.colors.textWarning,
        },
        info: {
            bg: theme.colors.bgSurfaceInfo,
            bgHover: theme.colors.bgSurfaceInformationHover,
            icon: theme.colors.iconInformation,
            text: theme.colors.textInformation,
        },
        loading: {
            bg: theme.colors.bgSurfaceInfo,
            bgHover: theme.colors.bgSurfaceInformationHover,
            icon: theme.colors.iconInformation,
            text: theme.colors.textInformation,
        },
    };
    return map[variant];
}

export const ToastContainer = styled.div`
    position: fixed;
    top: ${spacing.md};
    right: ${spacing.md};
    z-index: ${zIndices.toast};
    display: flex;
    flex-direction: column;
    gap: ${spacing.xsm};
    pointer-events: none;
`;

export const ToastWrapper = styled.div<{ $variant: ToastVariant; $exiting?: boolean }>`
    display: flex;
    align-items: center;
    gap: ${spacing.xsm};
    padding: ${spacing.sm} ${spacing.md};
    border-radius: ${radius.md};
    min-width: 240px;
    max-width: 420px;
    box-shadow: ${({ theme }) => theme.colors.shadowMd};
    pointer-events: auto;
    animation: ${({ $exiting }) => ($exiting ? slideOut : slideIn)} ${ANIMATION_DURATION_MS}ms ease forwards;

    ${({ theme, $variant }) => {
        const v = getVariantColors(theme, $variant);
        return css`
            background: ${v.bg};
            color: ${v.icon};

            .toast-icon {
                color: ${v.icon};
                ${$variant === 'loading'
                    ? css`
                          animation: ${spin} 1s linear infinite;
                      `
                    : ''}
            }

            .toast-text {
                color: ${v.text};
            }

            .toast-btn:hover {
                background: ${v.bgHover};
            }
        `;
    }}
`;

export const ToastBody = styled.div`
    display: flex;
    align-items: center;
    gap: ${spacing.sm};
    flex: 1;
    font-size: ${typography.fontSizes.md};
    font-weight: ${typography.fontWeights.medium};
    line-height: 1.4;

    .toast-icon {
        display: flex;
        flex-shrink: 0;
    }
`;

const toastButton = css`
    all: unset;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    width: 28px;
    height: 28px;
    border-radius: ${radius.md};
    color: inherit;
    opacity: 0.7;
    transition:
        opacity 0.15s,
        background 0.15s;

    &:hover {
        opacity: 1;
    }

    &:focus-visible {
        outline: 2px solid currentColor;
        outline-offset: 2px;
    }
`;

export const ToastActionButton = styled.button`
    ${toastButton}
`;

export const ToastCloseButton = styled.button`
    ${toastButton}
`;
