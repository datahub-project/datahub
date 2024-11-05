import React from 'react';
import { Menu, MenuItemProps } from 'antd';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';
import { NavBarMenuBaseItem } from './types';

const StyledMenuItem = styled(Menu.Item)`
    &&& {
        padding: 8px;
        margin: 0;
        margin-bottom: 0;
        height: 36px;
        border-radius: 6px;
        border: 0;
        display: flex;
        align-items: center;
    }

    && svg {
        color: #8088a3;
        width: 24px;
        height: 24px;
    }

    && .ant-menu-title-content {
        color: #5f6685;
        font-family: Mulish;
        font-size: 14px;
        font-style: normal;
        font-weight: 500;
        line-height: 36px;
        display: flex;
        height: 36px;
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

        && .filled {
            fill: #8088a3;
        }
    }
`;

type Props = {
    item: NavBarMenuBaseItem;
} & MenuItemProps;

export default function NavBarMenuItem({ item, ...props }: Props) {
    const history = useHistory();

    const redirect = (link: string | undefined) => link && history.push(link);

    const component = (
        <StyledMenuItem icon={item.icon} onClick={() => redirect(item.link)} {...props}>
            {item.title}
        </StyledMenuItem>
    );

    return component;
}
