import { Icon, Text, Tooltip } from '@components';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { MenuItemRendererProps } from '@components/components/Menu/types';
import { FontColorLevelOptions, FontColorOptions } from '@components/theme/config';
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
    titleColorLevel: FontColorLevelOptions;
    descriptionColor: FontColorOptions;
    descriptionColorLevel: FontColorLevelOptions;
    iconColor: FontColorOptions;
    iconColorLevel: FontColorLevelOptions;
}

const DEFAULT_COLORS: Colors = {
    titleColor: 'gray',
    titleColorLevel: 600,
    descriptionColor: 'gray',
    descriptionColorLevel: 1700,
    iconColor: 'gray',
    iconColorLevel: 1800,
};

const DISABLED_COLORS: Colors = {
    ...DEFAULT_COLORS,
    titleColorLevel: 300,
    descriptionColorLevel: 300,
    iconColorLevel: 300,
};

const DANGER_COLORS: Colors = {
    titleColor: 'red',
    titleColorLevel: 1000,
    descriptionColor: 'red',
    descriptionColorLevel: 600,
    iconColor: 'red',
    iconColorLevel: 600,
};

const DANGER_DISABLED_COLORS: Colors = {
    ...DANGER_COLORS,
    titleColorLevel: 300,
    descriptionColorLevel: 300,
    iconColorLevel: 300,
};

export default function MenuItemRenderer({ item }: MenuItemRendererProps) {
    const itemColors = useMemo(() => {
        if (item.disabled && !item.danger) return DISABLED_COLORS;
        if (!item.disabled && item.danger) return DANGER_COLORS;
        if (item.disabled && item.danger) return DANGER_DISABLED_COLORS;

        return DEFAULT_COLORS;
    }, [item.danger, item.disabled]);

    const content = (
        <Wrapper>
            {item.icon && (
                <IconWrapper>
                    <Icon
                        icon={item.icon}
                        source="phosphor"
                        color={itemColors.iconColor}
                        colorLevel={itemColors.iconColorLevel}
                        size="2xl"
                    />
                </IconWrapper>
            )}

            <Container>
                <Text weight="semiBold" color={itemColors.titleColor} colorLevel={itemColors.titleColorLevel}>
                    {item.title}
                </Text>
                {item.description && (
                    <Text color={itemColors.descriptionColor} colorLevel={itemColors.descriptionColorLevel} size="sm">
                        {item.description}
                    </Text>
                )}
            </Container>

            <SpaceFiller />

            {item.children && <Icon icon="CaretRight" source="phosphor" color="gray" colorLevel={1800} size="lg" />}
        </Wrapper>
    );

    if (item.tooltip) {
        return <Tooltip title={item.tooltip}>{content}</Tooltip>;
    }

    return content;
}
