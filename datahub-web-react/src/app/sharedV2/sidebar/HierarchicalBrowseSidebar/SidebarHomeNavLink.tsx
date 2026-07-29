import { House } from '@phosphor-icons/react/dist/csr/House';
import React from 'react';

import {
    HomeNavIcon,
    HomeNavLabel,
    HomeNavLink,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';

type Props = {
    to: string;
    isSelected: boolean;
    label: string;
    'data-testid'?: string;
};

export default function SidebarHomeNavLink({ to, isSelected, label, 'data-testid': dataTestId }: Props) {
    return (
        <HomeNavLink to={to} $isSelected={isSelected} data-testid={dataTestId}>
            <HomeNavIcon $isSelected={isSelected}>
                <House size={TREE_ROW_ENTITY_ICON_SIZE} weight={isSelected ? 'fill' : 'regular'} />
            </HomeNavIcon>
            <HomeNavLabel $isSelected={isSelected}>{label}</HomeNavLabel>
        </HomeNavLink>
    );
}
