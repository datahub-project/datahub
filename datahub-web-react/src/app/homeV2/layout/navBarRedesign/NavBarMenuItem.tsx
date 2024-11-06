import React from 'react';
import { Menu, MenuItemProps } from 'antd';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';
import { Text } from '@src/alchemy-components';
import { NavBarMenuBaseItem } from './types';

const StyledMenuItem = styled(Menu.Item)<{ isCollapsed?: boolean }>`
    &&& {
        padding: 4px 8px;
        margin: 0;
        margin-bottom: 0;
        height: 36px;
        border-radius: 6px;
        border: 0;
        display: flex;
        align-items: center;
        ${(props) => props.isCollapsed && 'width: 36px;'}
    }

    && svg {
        color: #8088a3;
        width: 20px;
        height: 20px;
    }

    && .ant-menu-title-content {
        color: #5f6685;
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

    &:hover {
        background: linear-gradient(
            180deg,
            rgba(243, 244, 246, 0.5) -3.99%,
            rgba(235, 236, 240, 0.5) 53.04%,
            rgba(235, 236, 240, 0.5) 100%
        );
        box-shadow: 0px 0px 0px 1px rgba(139, 135, 157, 0.08);
    }

    &&.ant-menu-item-selected {
        background: linear-gradient(
            180deg,
            rgba(83, 63, 209, 0.04) -3.99%,
            rgba(112, 94, 228, 0.04) 53.04%,
            rgba(112, 94, 228, 0.04) 100%
        );
        box-shadow: 0px 0px 0px 1px rgba(108, 71, 255, 0.08);

        && .ant-menu-title-content {
            color: #533fd1;
        }

        && svg {
            color: #533fd1;
        }
    }
`;

const Icon = styled.div`
    width: 20px;
    height: 20px;
`;

type Props = {
    item: NavBarMenuBaseItem;
    isCollapsed?: boolean;
    isSelected?: boolean;
} & MenuItemProps;

export default function NavBarMenuItem({ item, isCollapsed, isSelected, ...props }: Props) {
    const history = useHistory();

    const redirect = (link: string | undefined) => link && history.push(link);

    const component = (
        <StyledMenuItem isCollapsed={isCollapsed} onClick={() => redirect(item.link)} {...props}>
            <Icon>{isSelected ? item.selectedIcon || item.icon : item.icon}</Icon>
            {!isCollapsed && (
                <Text size="md" type="div">
                    {item.title}
                </Text>
            )}
        </StyledMenuItem>
    );

    return component;
}
