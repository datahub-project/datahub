import { Icon, Text, Tooltip } from '@components';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React from 'react';
import styled from 'styled-components';

import { MenuItemRendererProps } from '@components/components/Menu/types';
import spacing from '@components/theme/foundations/spacing';

const Wrapper = styled.div`
    display: flex;
    gap: ${spacing.xsm};
    padding: ${spacing.xsm};
    align-items: center;
`;

const Container = styled.div<{ $disabled?: boolean; $danger?: boolean }>`
    display: flex;
    flex-direction: column;
    text-overflow: ellipsis;
    word-wrap: nowrap;
    color: ${(props) => {
        if (props.$disabled) return props.theme.colors.textDisabled;
        if (props.$danger) return props.theme.colors.textError;
        return props.theme.colors.text;
    }};
`;

const IconWrapper = styled.div<{ $disabled?: boolean; $danger?: boolean }>`
    display: flex;
    flex-shrink: 0;
    color: ${(props) => {
        if (props.$disabled) return props.theme.colors.iconDisabled;
        if (props.$danger) return props.theme.colors.iconError;
        return props.theme.colors.icon;
    }};
`;

const SpaceFiller = styled.div`
    flex-grow: 1;
`;

const Description = styled.div<{ $disabled?: boolean; $danger?: boolean }>`
    color: ${(props) => {
        if (props.$disabled) return props.theme.colors.textDisabled;
        if (props.$danger) return props.theme.colors.textError;
        return props.theme.colors.textSecondary;
    }};
`;

const Caret = styled.div`
    display: flex;
    color: ${(props) => props.theme.colors.icon};
`;

export default function MenuItemRenderer({ item }: MenuItemRendererProps) {
    const content = (
        <Wrapper data-testid={item.dataTestId || `menu-item-${item.key}`}>
            {item.icon && (
                <IconWrapper $disabled={item.disabled} $danger={item.danger}>
                    <Icon icon={item.icon} size="2xl" />
                </IconWrapper>
            )}

            <Container $disabled={item.disabled} $danger={item.danger}>
                <Text weight="semiBold">{item.title}</Text>
                {item.description && (
                    <Description $disabled={item.disabled} $danger={item.danger}>
                        <Text size="sm">{item.description}</Text>
                    </Description>
                )}
            </Container>

            <SpaceFiller />

            {item.children && (
                <Caret>
                    <Icon icon={CaretRight} size="lg" />
                </Caret>
            )}
        </Wrapper>
    );

    if (item.tooltip) {
        return (
            <Tooltip title={item.tooltip} placement={item.tooltipPlacement}>
                {content}
            </Tooltip>
        );
    }

    return content;
}
