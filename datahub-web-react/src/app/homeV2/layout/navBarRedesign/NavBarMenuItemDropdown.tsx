import React from 'react';
import { Dropdown, MenuItemProps } from 'antd';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { colors, Text } from '@src/alchemy-components';
import NavBarMenuItem from './NavBarMenuItem';
import { NavBarMenuDropdownItem } from './types';

const StyledDropdownContentWrapper = styled.div`
    background-color: white;
    border-radius: ${(props) => props.theme.styles['border-radius-navbar-redesign']};
    box-shadow: 0px 0px 14px 0px rgba(0, 0, 0, 0.15);
    padding: 8px;
`;

const StyledDropDownOption = styled.div<{ $disabled?: boolean }>`
    padding: 8px;
    border-radius: ${(props) => props.theme.styles['border-radius-navbar-redesign']};
    ${(props) =>
        props.$disabled
            ? `color: ${colors.gray[1800]};`
            : `
                cursor: pointer;
                &:hover {
                    background: linear-gradient(
                        180deg,
                        rgba(243, 244, 246, 0.5) -3.99%,
                        rgba(235, 236, 240, 0.5) 53.04%,
                        rgba(235, 236, 240, 0.5) 100%
                    );
                }
    `}
`;

type Props = {
    item: NavBarMenuDropdownItem;
    isCollapsed?: boolean;
    isSelected?: boolean;
} & MenuItemProps;

export default function NavBarMenuItemDropdown({ item, isCollapsed, isSelected, ...props }: Props) {
    const history = useHistory();

    const dropdownItems = item.items?.filter((subItem) => !subItem.isHidden);

    const onItemClick = (key) => {
        const clickedItem = item.items?.filter((dropdownItem) => dropdownItem.key === key)?.[0];
        if (!clickedItem) return null;

        if (clickedItem.disabled) return null;

        if (clickedItem.onClick) return clickedItem.onClick();

        if (clickedItem.link && clickedItem.isExternalLink)
            return window.open(clickedItem.link, '_blank', 'noopener,noreferrer');
        if (clickedItem.link && !clickedItem.isExternalLink) return history.push(clickedItem.link);

        return null;
    };

    return (
        <Dropdown
            dropdownRender={() => {
                return (
                    <StyledDropdownContentWrapper>
                        {dropdownItems?.map((dropdownItem) => {
                            return (
                                <StyledDropDownOption
                                    key={dropdownItem.key}
                                    $disabled={dropdownItem.disabled}
                                    role="menuitem"
                                    aria-label={dropdownItem.title}
                                    aria-disabled={dropdownItem.disabled}
                                    onClick={() => onItemClick(dropdownItem.key)}
                                >
                                    <Text>{dropdownItem.title}</Text>
                                    <Text size="sm" color="gray">
                                        {dropdownItem.description}
                                    </Text>
                                </StyledDropDownOption>
                            );
                        })}
                    </StyledDropdownContentWrapper>
                );
            }}
        >
            <NavBarMenuItem item={item} isCollapsed={isCollapsed} isSelected={isSelected} {...props} />
        </Dropdown>
    );
}
