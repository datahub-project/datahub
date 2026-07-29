import { Tooltip } from '@components';
import { House } from '@phosphor-icons/react/dist/csr/House';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import React from 'react';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import {
    CollapsedScrollColumn,
    SearchIconButton,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseSidebar.components';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { treeRowInteractionBg } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/treeRow.styles';

/**
 * Hit target + selected chrome for collapsed icons. Uses the same ::before
 * interaction paint as expanded tree rows so Glossary’s flat rail and Domains’
 * collapsed tree stay on one rhythm.
 */
const CollapsedIconLink = styled(Link)<{ $isSelected?: boolean }>`
    position: relative;
    z-index: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    width: 100%;
    min-height: 38px;
    height: 38px;
    margin: 0 0 2px 0;
    padding: 0;
    text-decoration: none;
    background: transparent;
    ${treeRowInteractionBg}
`;

export type CollapsedIconRailItem = {
    key: string;
    to: string;
    label: string;
    isSelected: boolean;
    icon: React.ReactNode;
};

export type CollapsedIconRailHome = {
    to: string;
    label: string;
    isSelected: boolean;
};

export type SidebarCollapsedIconRailProps = {
    onExpandSearch: () => void;
    searchAriaLabel?: string;
    searchTestId?: string;
    home?: CollapsedIconRailHome;
    items: CollapsedIconRailItem[];
};

/**
 * Shared collapsed sidebar chrome: search (re-expands) + optional home + icon column.
 * Pages only supply the icons/links — sizing, spacing, and selected focus live here.
 */
export default function SidebarCollapsedIconRail({
    onExpandSearch,
    searchAriaLabel,
    searchTestId,
    home,
    items,
}: SidebarCollapsedIconRailProps) {
    const theme = useTheme();

    return (
        <>
            <SearchIconButton
                type="button"
                onClick={onExpandSearch}
                aria-label={searchAriaLabel}
                data-testid={searchTestId}
            >
                <MagnifyingGlass size={TREE_ROW_ENTITY_ICON_SIZE} weight="regular" />
            </SearchIconButton>
            <CollapsedScrollColumn>
                {home != null && (
                    <Tooltip title={home.label} placement="right" showArrow={false}>
                        <CollapsedIconLink to={home.to} $isSelected={home.isSelected}>
                            <House
                                size={TREE_ROW_ENTITY_ICON_SIZE}
                                weight={home.isSelected ? 'fill' : 'regular'}
                                color={home.isSelected ? theme.colors.iconBrand : theme.colors.icon}
                            />
                        </CollapsedIconLink>
                    </Tooltip>
                )}
                {items.map((item) => (
                    <Tooltip key={item.key} title={item.label} placement="right" showArrow={false}>
                        <CollapsedIconLink to={item.to} $isSelected={item.isSelected}>
                            {item.icon}
                        </CollapsedIconLink>
                    </Tooltip>
                ))}
            </CollapsedScrollColumn>
        </>
    );
}
