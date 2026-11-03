import React from 'react';

import { Button } from '@components/components/Button';
import {
    ActionsWrapper,
    Container,
    DescriptionText,
    IconWrapper,
    TextWrapper,
    TitleText,
} from '@components/components/EmptyState/components';
import { EmptyStateProps, EmptyStateSize } from '@components/components/EmptyState/types';
import { Icon } from '@components/components/Icon';
import { Text } from '@components/components/Text';
import { Tooltip } from '@components/components/Tooltip';
import { FontSizeOptions } from '@components/theme/config';

const ICON_SIZE: Record<EmptyStateSize, FontSizeOptions> = {
    sm: '2xl',
    default: '3xl',
    lg: '4xl',
};

function EmptyStateButton({
    action,
    variant,
}: {
    action: NonNullable<EmptyStateProps['action']>;
    variant: 'filled' | 'text';
}): React.ReactElement {
    const button = (
        <Button
            onClick={action.onClick}
            icon={action.icon}
            iconPosition={action.iconPosition}
            variant={action.variant ?? variant}
            size="md"
            disabled={action.disabled}
            data-testid={action.dataTestId}
        >
            {action.label}
        </Button>
    );

    if (!action.tooltip) {
        return button;
    }

    return (
        <Tooltip title={action.tooltip} showArrow={false}>
            <span>{button}</span>
        </Tooltip>
    );
}

export function EmptyState({
    title,
    description,
    icon,
    image,
    action,
    secondaryAction,
    size = 'default',
    className,
    style,
}: EmptyStateProps) {
    return (
        <Container $size={size} className={className} style={style}>
            {image && <IconWrapper>{image}</IconWrapper>}
            {!image && icon && (
                <IconWrapper>
                    <Icon icon={icon} size={ICON_SIZE[size]} />
                </IconWrapper>
            )}
            <TextWrapper>
                <TitleText>
                    <Text weight="semiBold" size={size === 'sm' ? 'md' : 'lg'} color="inherit">
                        {title}
                    </Text>
                </TitleText>
                {description && (
                    <DescriptionText>
                        <Text size="sm" color="inherit">
                            {description}
                        </Text>
                    </DescriptionText>
                )}
            </TextWrapper>
            {(action || secondaryAction) && (
                <ActionsWrapper>
                    {action && <EmptyStateButton action={action} variant="filled" />}
                    {secondaryAction && <EmptyStateButton action={secondaryAction} variant="text" />}
                </ActionsWrapper>
            )}
        </Container>
    );
}
