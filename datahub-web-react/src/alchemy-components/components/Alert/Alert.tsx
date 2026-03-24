import { CheckCircle } from '@phosphor-icons/react/dist/csr/CheckCircle';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import { MegaphoneSimple } from '@phosphor-icons/react/dist/csr/MegaphoneSimple';
import { WarningCircle } from '@phosphor-icons/react/dist/csr/WarningCircle';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React from 'react';

import { AlertActions, AlertContainer, AlertContent, AlertIconWrapper } from '@components/components/Alert/components';
import { AlertProps, AlertVariant } from '@components/components/Alert/types';
import { Text } from '@components/components/Text';

const DEFAULT_ICONS: Record<AlertVariant, React.ReactNode> = {
    success: <CheckCircle size={20} weight="fill" />,
    error: <WarningCircle size={20} weight="fill" />,
    warning: <WarningCircle size={20} weight="fill" />,
    info: <Info size={20} weight="fill" />,
    brand: <MegaphoneSimple size={20} weight="fill" />,
};

/**
 * Inline status banner for success, error, warning, info, and brand messages.
 * Colors are derived from semantic theme tokens based on the variant.
 */
export function Alert({ variant, title, description, icon, onClose, action, className, style }: AlertProps) {
    const displayIcon = icon ?? DEFAULT_ICONS[variant];

    return (
        <AlertContainer $variant={variant} className={className} style={style}>
            <AlertIconWrapper $variant={variant}>{displayIcon}</AlertIconWrapper>
            <AlertContent $variant={variant}>
                <Text weight="semiBold" size="md">
                    {title}
                </Text>
                {description && <Text size="sm">{description}</Text>}
            </AlertContent>
            {(action || onClose) && (
                <AlertActions>
                    {action}
                    {onClose && (
                        <button
                            type="button"
                            aria-label="close"
                            onClick={onClose}
                            style={{
                                all: 'unset',
                                cursor: 'pointer',
                                display: 'flex',
                                alignItems: 'center',
                                opacity: 0.7,
                            }}
                        >
                            <X size={16} weight="bold" />
                        </button>
                    )}
                </AlertActions>
            )}
        </AlertContainer>
    );
}
