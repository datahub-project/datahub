import { Icon, Text, Tooltip } from '@components';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { MenuItemRendererProps } from '@components/components/Menu/types';
import { FontColorOptions } from '@components/theme/config';
import spacing from '@components/theme/foundations/spacing';

const Wrapper = styled.div`
    display: flex;
    gap: ${spacing.xsm};
    padding: ${spacing.xsm};
    align-items: center;
`;

const Container = styled.div`
    display: flex;
    flex-direction: column;
    text-overflow: ellipsis;
    word-wrap: nowrap;
`;

const IconWrapper = styled.div`
    display: flex;
    flex-shrink: 0;
`;

const SpaceFiller = styled.div`
    flex-grow: 1;
`;

interface Colors {
    titleColor: FontColorOptions;
    descriptionColor: FontColorOptions;
    iconColor: FontColorOptions;
}

const DEFAULT_COLORS: Colors = {
    titleColor: 'text',
    descriptionColor: 'textSecondary',
    iconColor: 'icon',
};

const DISABLED_COLORS: Colors = {
    titleColor: 'textDisabled',
    descriptionColor: 'textDisabled',
    iconColor: 'iconDisabled',
};

const DANGER_COLORS: Colors = {
    titleColor: 'red',
    descriptionColor: 'red',
    iconColor: 'red',
};

const DANGER_DISABLED_COLORS: Colors = {
    titleColor: 'textDisabled',
    descriptionColor: 'textDisabled',
    iconColor: 'iconDisabled',
};

export default function MenuItemRenderer({ item }: MenuItemRendererProps) {
    const itemColors = useMemo(() => {
        if (item.disabled && !item.danger) return DISABLED_COLORS;
        if (!item.disabled && item.danger) return DANGER_COLORS;
        if (item.disabled && item.danger) return DANGER_DISABLED_COLORS;

        return DEFAULT_COLORS;
    }, [item.danger, item.disabled]);

    const content = (
        <Wrapper data-testid={item.dataTestId || `menu-item-${item.key}`}>
            {item.icon && (
                <IconWrapper>
                    <Icon icon={item.icon} color={itemColors.iconColor} size="2xl" />
                </IconWrapper>
            )}

            <Container>
                <Text weight="semiBold" color={itemColors.titleColor}>
                    {item.title}
                </Text>
                {item.description && (
                    <Text color={itemColors.descriptionColor} size="sm">
                        {item.description}
                    </Text>
                )}
            </Container>

            <SpaceFiller />

            {item.children && <Icon icon={CaretRight} color="icon" size="lg" />}
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
