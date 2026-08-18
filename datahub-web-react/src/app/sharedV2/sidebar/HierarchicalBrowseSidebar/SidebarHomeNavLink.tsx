import { House } from '@phosphor-icons/react/dist/csr/House';
import React from 'react';
import { useTheme } from 'styled-components';

import { HomeNavLink } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { TreeRowIconSlot, TreeRowTitle } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/treeRow.styles';

type Props = {
    to: string;
    isSelected: boolean;
    label: string;
    'data-testid'?: string;
};

export default function SidebarHomeNavLink({ to, isSelected, label, 'data-testid': dataTestId }: Props) {
    const theme = useTheme();
    return (
        <HomeNavLink to={to} $isSelected={isSelected} data-testid={dataTestId}>
            <TreeRowIconSlot>
                <House
                    size={TREE_ROW_ENTITY_ICON_SIZE}
                    weight={isSelected ? 'fill' : 'regular'}
                    color={isSelected ? theme.colors.iconBrand : theme.colors.icon}
                />
            </TreeRowIconSlot>
            <TreeRowTitle $isSelected={isSelected}>{label}</TreeRowTitle>
        </HomeNavLink>
    );
}
