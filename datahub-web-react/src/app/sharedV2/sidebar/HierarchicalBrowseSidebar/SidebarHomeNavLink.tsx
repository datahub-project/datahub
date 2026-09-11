import type { Icon } from '@phosphor-icons/react';
import { House } from '@phosphor-icons/react/dist/csr/House';
import React from 'react';
import { useTheme } from 'styled-components';

import {
    HomeNavButton,
    HomeNavLink,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { TreeRowIconSlot, TreeRowTitle } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/treeRow.styles';

type SharedProps = {
    isSelected: boolean;
    label: string;
    icon?: Icon;
    'data-testid'?: string;
};

type Props = SharedProps & ({ to: string; onClick?: undefined } | { onClick: () => void; to?: undefined });

export default function SidebarHomeNavLink({ to, onClick, isSelected, label, icon, 'data-testid': dataTestId }: Props) {
    const theme = useTheme();
    const IconComponent = icon ?? House;
    const content = (
        <>
            <TreeRowIconSlot>
                <IconComponent
                    size={TREE_ROW_ENTITY_ICON_SIZE}
                    weight={isSelected ? 'fill' : 'regular'}
                    color={isSelected ? theme.colors.iconBrand : theme.colors.icon}
                />
            </TreeRowIconSlot>
            <TreeRowTitle $isSelected={isSelected}>{label}</TreeRowTitle>
        </>
    );

    if (onClick) {
        return (
            <HomeNavButton type="button" $isSelected={isSelected} onClick={onClick} data-testid={dataTestId}>
                {content}
            </HomeNavButton>
        );
    }

    if (!to) {
        return null;
    }

    return (
        <HomeNavLink to={to} $isSelected={isSelected} data-testid={dataTestId}>
            {content}
        </HomeNavLink>
    );
}
