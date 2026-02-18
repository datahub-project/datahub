import { Menu, MenuItemProps, Tooltip } from 'antd';
import React from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { getColor } from '@components/theme/utils';

import { NavBarMenuBaseItem } from '@app/homeV2/layout/navBarRedesign/types';
import { Badge, Text } from '@src/alchemy-components';
import analytics, { EventType } from '@src/app/analytics';

const StyledMenuItem = styled(Menu.Item)<{ isCollapsed?: boolean }>`
    &&& {
        position: relative;
        padding: 4px 8px;
        margin: 8px 0;
        margin-bottom: 0;
        height: 36px;
        min-height: 36px;
        border-radius: 6px;
        border: 0;
        display: flex;
        align-items: center;
        ${(props) => props.isCollapsed && 'width: 36px;'}
        @media (max-height: 970px) {
            margin: 2px 0;
        }
        @media (max-height: 890px) {
            margin: 0;
        }
    }

    && svg {
        color: ${(props) => props.theme.colors.textTertiary};
        width: 20px;
        height: 20px;
    }

    && .ant-menu-title-content {
        width: 100%;
        color: ${(props) => props.theme.colors.textSecondary};
        font-family: Mulish;
        font-size: 14px;
        font-style: normal;
        font-weight: 500;
        line-height: 36px;
        display: flex;
        gap: 8px;
        align-items: center;
        height: 36px;
        line-height: 24px;
    }

    &:hover,
    &.ant-menu-item-active {
        background: ${(props) => props.theme.colors.bgHover};
        box-shadow: ${(props) => props.theme.colors.shadowFocus};
    }

    &&.ant-menu-item-selected {
        background: ${(props) => props.theme.colors.bgSelectedSubtle};
        box-shadow: ${(props) => props.theme.colors.shadowFocusBrand};
    }
`;

const Icon = styled.div<{ $isSelected?: boolean; $size?: number }>`
    width: ${(props) => props.$size ?? 20}px;
    height: ${(props) => props.$size ?? 20}px;

    && svg {
        ${(props) =>
            props.$isSelected
                ? `fill: url(#menu-item-selected-gradient) ${props.theme.styles['primary-color']};`
                : `color: ${props.theme.colors.icon};`}
        width: ${(props) => props.$size ?? 20}px;
        height: ${(props) => props.$size ?? 20}px;
    }
`;

const StyledText = styled(Text)<{ $isSelected?: boolean }>`
    ${(props) =>
        props.$isSelected &&
        `
        background: linear-gradient(${getColor('primary', 300, props.theme)} 1%, ${getColor(
            'primary',
            500,
            props.theme,
        )} 99%);
        background-clip: text;
        -webkit-text-fill-color: transparent;
    `}
`;

const ItemTitleContentWrapper = styled.div`
    width: 100%;
    display: flex;
    justify-content: space-between;
`;

const PillDot = styled.div<{ $isSelected?: boolean }>`
    position: absolute;
    width: 10px;
    height: 10px;
    background: ${(props) => props.theme.styles['primary-color']};
    border-radius: 6px;
    border: 2px solid ${(props) => props.theme.colors.bgSurfaceNewNav};
    top: 6px;
    left: 22px;
`;

type Props = {
    item: NavBarMenuBaseItem;
    isCollapsed?: boolean;
    isSelected?: boolean;
    iconSize?: number;
} & MenuItemProps;

export default function NavBarMenuItem({ item, isCollapsed, isSelected, iconSize, ...props }: Props) {
    const history = useHistory();

    const onClick = () => {
        analytics.event({ type: EventType.NavBarItemClick, label: item.title });
        if (item.onClick) item.onClick();
        if (item.link) return history.push(item.link);
        return null;
    };

    const component = (
        <Tooltip title={isCollapsed ? item.title : null} placement="right" showArrow={false}>
            <StyledMenuItem
                isCollapsed={isCollapsed}
                onClick={onClick}
                aria-label={item.title}
                {...props}
                data-testid={item.dataTestId}
            >
                {item.icon || item.selectedIcon ? (
                    <Icon $size={iconSize} $isSelected={isSelected}>
                        {isSelected ? item.selectedIcon || item.icon : item.icon}
                    </Icon>
                ) : null}
                {isCollapsed ? (
                    <>{item?.badge?.show && <PillDot />}</>
                ) : (
                    <ItemTitleContentWrapper>
                        <StyledText size="md" type="div" weight="semiBold" $isSelected={isSelected}>
                            {item.title}
                        </StyledText>
                        {item?.badge?.show && <Badge count={item.badge.count} clickable={false} color="primary" />}
                    </ItemTitleContentWrapper>
                )}
            </StyledMenuItem>
        </Tooltip>
    );

    if (item.href) {
        return <a href={item.href}>{component}</a>;
    }

    return component;
}
