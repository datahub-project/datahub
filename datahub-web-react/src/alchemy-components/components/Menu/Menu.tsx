import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';

import { Dropdown } from '@components/components/Dropdown';
import useConvertedAntdMenu from '@components/components/Menu/hooks/useConvertedAntdMenu';
import { MenuProps } from '@components/components/Menu/types';

const StyledDropdownContainer = styled.div`
    max-width: 335px;

    &&& .ant-dropdown-menu {
        border-radius: 12px;
        background-color: ${(props) => props.theme.colors.bgOverlay};
        box-shadow: ${(props) => props.theme.colors.shadowMd};
    }

    &&& .ant-dropdown-menu-item,
    &&& .ant-dropdown-menu-submenu-title,
    &&& .ant-dropdown-menu-item-group-title {
        color: ${(props) => props.theme.colors.text};
    }

    &&& .ant-dropdown-menu-item:hover,
    &&& .ant-dropdown-menu-item-active,
    &&& .ant-dropdown-menu-submenu-title:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }

    &&& .ant-dropdown-menu-item-disabled,
    &&& .ant-dropdown-menu-item-disabled:hover {
        color: ${(props) => props.theme.colors.textDisabled};
        background-color: transparent;
    }

    &&& .ant-dropdown-menu-item-divider {
        background-color: ${(props) => props.theme.colors.border};
    }

    &&& {
        .ant-dropdown-menu-sub {
            border-radius: 12px;
            background-color: ${(props) => props.theme.colors.bgOverlay};
        }
    }
`;

export function Menu({ children, items, open, onOpenChange, ...props }: React.PropsWithChildren<MenuProps>) {
    const convertedMenu = useConvertedAntdMenu(items);

    // Submenus are portaled outside the dropdown, so hiding the dropdown (e.g. with Escape) leaves an
    // open submenu on screen. Their open state is controlled here so it can be cleared with the menu.
    const [openKeys, setOpenKeys] = useState<string[]>([]);

    useEffect(() => {
        if (open === false) setOpenKeys([]);
    }, [open]);

    const handleOpenChange = useCallback(
        (isOpen: boolean) => {
            if (!isOpen) setOpenKeys([]);
            onOpenChange?.(isOpen);
        },
        [onOpenChange],
    );

    const menu = useMemo(() => ({ ...convertedMenu, openKeys, onOpenChange: setOpenKeys }), [convertedMenu, openKeys]);

    return (
        <Dropdown
            {...props}
            open={open}
            onOpenChange={handleOpenChange}
            dropdownRender={(originNode) => <StyledDropdownContainer>{originNode}</StyledDropdownContainer>}
            menu={menu}
            resetDefaultMenuStyles
        >
            {children}
        </Dropdown>
    );
}
