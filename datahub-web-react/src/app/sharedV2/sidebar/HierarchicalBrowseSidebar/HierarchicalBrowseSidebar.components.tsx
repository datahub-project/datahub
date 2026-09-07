import { Button } from '@components';
import { Link } from 'react-router-dom';
import styled, { css } from 'styled-components';

import {
    SIDEBAR_COLLAPSED_WIDTH,
    SIDEBAR_TRANSITION_MS,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { treeRowHitTarget, treeRowInteractionBg } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/treeRow.styles';

const scrollbar = css`
    scrollbar-gutter: stable;
    &::-webkit-scrollbar {
        width: 6px;
    }
    &::-webkit-scrollbar-track {
        background: ${(props) => props.theme.colors.scrollbarTrack};
    }
    &::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.scrollbarThumb};
        border-radius: 3px;
    }
    &::-webkit-scrollbar-thumb:hover {
        background: ${(props) => props.theme.colors.scrollbarThumbHover};
    }
    scrollbar-width: thin;
    scrollbar-color: ${(props) => `${props.theme.colors.scrollbarThumb} ${props.theme.colors.scrollbarTrack}`};
`;

export const SidebarCreateButton = styled(Button)`
    padding: 2px;
    svg {
        width: 20px;
        height: 20px;
    }
`;

export const SearchSlot = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    flex-shrink: 0;
    position: relative;
    padding: 12px;
`;

/** Flexes so SearchBar / autocomplete fills the row beside an optional sort control. */
export const SearchControl = styled.div`
    flex: 1;
    min-width: 0;
    position: relative;
`;

export const SortSlot = styled.div`
    flex-shrink: 0;
    display: flex;
    align-items: center;
`;

export const HomeNavSlot = styled.div<{ $inTree?: boolean }>`
    flex-shrink: 0;
    padding: ${(props) => (props.$inTree ? '0' : '0 8px 8px')};
`;

export const SearchResultsDropdown = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 5px;
    box-shadow: ${(props) => props.theme.colors.shadowMd};
    padding: 8px;
    position: absolute;
    max-height: 210px;
    overflow: auto;
    width: 100%;
    left: 0;
    top: calc(100% + 4px);
    z-index: 1;
`;

export const CollapsedScrollColumn = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    overflow-x: hidden;
    padding: 8px;
    ${scrollbar}
`;

export const SidebarShell = styled.div`
    position: relative;
    flex-shrink: 0;
    align-self: stretch;
    height: 100%;
    max-height: 100%;
`;

export const SidebarContainer = styled.div<{
    $isCollapsed: boolean;
    $width: number;
    $isShowNavBarRedesign?: boolean;
    $isResizing?: boolean;
}>`
    position: relative;
    flex-shrink: 0;
    height: 100%;
    max-height: 100%;
    align-self: stretch;
    width: ${(props) => (props.$isCollapsed ? `${SIDEBAR_COLLAPSED_WIDTH}px` : `${props.$width}px`)};
    min-width: ${(props) => (props.$isCollapsed ? `${SIDEBAR_COLLAPSED_WIDTH}px` : `${props.$width}px`)};
    transition: ${(props) =>
        props.$isResizing
            ? 'none'
            : `width ${SIDEBAR_TRANSITION_MS}ms ease-in-out, min-width ${SIDEBAR_TRANSITION_MS}ms ease-in-out`};
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    display: flex;
    flex-direction: column;
    overflow: hidden;
    ${(props) => !props.$isShowNavBarRedesign && 'margin-bottom: 12px;'}
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
            margin: 0;
            box-shadow: ${props.theme.colors.shadowSm};
        `}
`;

export const HeaderControls = styled.div<{ $isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.$isCollapsed ? 'center' : 'space-between')};
    padding: 12px;
    height: 50px;
    overflow: hidden;
    gap: 8px;
`;

export const SidebarTitle = styled.div`
    font-size: 16px;
    font-weight: bold;
    color: ${(props) => props.theme.colors.text};
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex: 1;
    min-width: 0;
`;

export const HeaderButtons = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    flex-shrink: 0;
`;

export const ThinDivider = styled.div`
    height: 1px;
    flex-shrink: 0;
    background: ${(props) => props.theme.colors.border};
`;

export const SearchIconButton = styled.button`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 100%;
    padding: 16px 0;
    border: none;
    background: transparent;
    cursor: pointer;
    color: ${(props) => props.theme.colors.icon};

    &:hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
`;

export const FiltersRow = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    padding: 0 12px 12px;
    flex-shrink: 0;
`;

export const TreeContainer = styled.div`
    flex: 1;
    overflow-y: auto;
    overflow-x: hidden;
    /* Equal L/R inset — asymmetric 8/2 looked lopsided inside the white shell. */
    padding: 8px;
    ${scrollbar}
`;

export const Content = styled.div`
    display: flex;
    flex-direction: column;
    overflow: hidden;
    flex: 1;
    min-height: 0;
`;

export const HomeNavLink = styled(Link)<{ $isSelected: boolean }>`
    ${treeRowHitTarget}
    display: flex;
    align-items: center;
    padding: 4px 8px;
    text-decoration: none;
    cursor: pointer;
    ${treeRowInteractionBg}
`;
