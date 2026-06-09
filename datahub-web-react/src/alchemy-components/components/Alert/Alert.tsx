import { CheckCircle } from '@phosphor-icons/react/dist/csr/CheckCircle';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import { MegaphoneSimple } from '@phosphor-icons/react/dist/csr/MegaphoneSimple';
import { Question } from '@phosphor-icons/react/dist/csr/Question';
import { WarningCircle } from '@phosphor-icons/react/dist/csr/WarningCircle';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React from 'react';
import { useTranslation } from 'react-i18next';

import {
    AlertActions,
    AlertBody,
    AlertContainer,
    AlertErrorMessage,
    AlertHeader,
    AlertHeaderLeft,
    AlertHeaderRight,
    AlertIconWrapper,
} from '@components/components/Alert/components';
import { AlertProps, AlertVariant } from '@components/components/Alert/types';
import { Button } from '@components/components/Button';
import { Text } from '@components/components/Text';
import type { ColorOptions } from '@components/theme/config';

const DEFAULT_ICONS: Record<AlertVariant, React.ReactNode> = {
    success: <CheckCircle size={20} weight="fill" />,
    error: <WarningCircle size={20} weight="fill" />,
    warning: <WarningCircle size={20} weight="fill" />,
    info: <Info size={20} weight="fill" />,
    brand: <MegaphoneSimple size={20} weight="fill" />,
    unknown: <Question size={20} weight="fill" />,
};

/**
 * Mapping of Alert variant -> alchemy Button color, so that any in-Alert button
 * (close button, action slot) reads as the same hue as the surrounding banner.
 */
export const VARIANT_BUTTON_COLOR_MAP: Record<AlertVariant, ColorOptions> = {
    success: 'green',
    error: 'red',
    warning: 'yellow',
    info: 'blue',
    brand: 'violet',
    unknown: 'gray',
};

/**
 * Inline status banner for success, error, warning, info, brand, and unknown
 * messages. Layout is a header row (icon + title on the left, optional
 * topRight action + close button on the right) with description, errorMessage,
 * and inline actions stacked below the header at full content width.
 *
 * Colors are derived from semantic theme tokens based on the variant.
 */
export function Alert({
    variant,
    title,
    description,
    errorMessage,
    icon,
    onClose,
    action,
    actionPlacement = 'inline',
    className,
    style,
    'data-testid': dataTestId,
}: AlertProps) {
    const { t: tc } = useTranslation('common.actions');
    const displayIcon = icon ?? DEFAULT_ICONS[variant];
    const showInlineAction = action && actionPlacement === 'inline';
    const showTopRightAction = action && actionPlacement === 'topRight';
    const hasHeaderRight = showTopRightAction || !!onClose;
    const hasBody = !!description || !!errorMessage || !!showInlineAction;

    return (
        <AlertContainer
            $variant={variant}
            $hasClose={!!onClose}
            className={className}
            style={style}
            data-testid={dataTestId}
        >
            <AlertHeader>
                <AlertHeaderLeft>
                    <AlertIconWrapper $variant={variant}>{displayIcon}</AlertIconWrapper>
                    <Text weight="semiBold" size="md">
                        {title}
                    </Text>
                </AlertHeaderLeft>
                {hasHeaderRight && (
                    <AlertHeaderRight>
                        {showTopRightAction && action}
                        {onClose && (
                            <Button
                                variant="text"
                                color={VARIANT_BUTTON_COLOR_MAP[variant]}
                                type="button"
                                aria-label={tc('close')}
                                onClick={onClose}
                            >
                                <X size={16} weight="bold" />
                            </Button>
                        )}
                    </AlertHeaderRight>
                )}
            </AlertHeader>
            {hasBody && (
                <AlertBody>
                    {description && <Text size="md">{description}</Text>}
                    {errorMessage && <AlertErrorMessage>{errorMessage}</AlertErrorMessage>}
                    {showInlineAction && <AlertActions>{action}</AlertActions>}
                </AlertBody>
            )}
        </AlertContainer>
    );
}
