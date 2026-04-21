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
import { FontSizeOptions } from '@components/theme/config';

const ICON_SIZE: Record<EmptyStateSize, FontSizeOptions> = {
    sm: '2xl',
    default: '3xl',
    lg: '4xl',
};

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
                    {action && (
                        <Button
                            onClick={action.onClick}
                            icon={action.icon}
                            variant={action.variant ?? 'filled'}
                            size="md"
                        >
                            {action.label}
                        </Button>
                    )}
                    {secondaryAction && (
                        <Button
                            onClick={secondaryAction.onClick}
                            icon={secondaryAction.icon}
                            variant={secondaryAction.variant ?? 'text'}
                            size="md"
                        >
                            {secondaryAction.label}
                        </Button>
                    )}
                </ActionsWrapper>
            )}
        </Container>
    );
}
