import { Button } from '@components';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import {
    SIDEBAR_COLLAPSED_WIDTH,
    SIDEBAR_TRANSITION_MS,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { treeRowInteractionBg } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/treeRow.styles';
import { hierarchicalBrowseScrollbar } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/utils/scrollbar.styles';

/** Header "+" create control — same chrome on Documents / Glossary / Domains. */
export const SidebarCreateButton = styled(Button)`
    padding: 2px;
    svg {
        width: 20px;
        height: 20px;
    }
`;

/** Shell-owned search padding so every page’s search control lines up. */
export const SearchSlot = styled.div`
    flex-shrink: 0;
    position: relative;
    padding: 12px;
`;

/** Home / overview row band — horizontal rhythm matches TreeContainer.
 *  Above the divider (no-filters layout) needs bottom margin so the home
 *  row doesn't sit flush against the ThinDivider. */
export const HomeNavSlot = styled.div<{ $inTree?: boolean }>`
    flex-shrink: 0;
    padding: ${(props) => (props.$inTree ? '0' : '0 8px 8px')};
`;

/** Shared autocomplete dropdown chrome for sidebar search. */
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

/** Collapsed icon column — same padding/scrollbar as TreeContainer. */
export const CollapsedScrollColumn = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    overflow-x: hidden;
    padding: 8px 2px 8px 2px;
    ${hierarchicalBrowseScrollbar}
`;

export const SidebarContainer = styled.div<{
    $isCollapsed: boolean;
    $width: number;
    $isShowNavBarRedesign?: boolean;
}>`
    flex-shrink: 0;
    height: 100%;
    max-height: 100%;
    align-self: stretch;
    width: ${(props) => (props.$isCollapsed ? `${SIDEBAR_COLLAPSED_WIDTH}px` : `${props.$width}px`)};
    min-width: ${(props) => (props.$isCollapsed ? `${SIDEBAR_COLLAPSED_WIDTH}px` : `${props.$width}px`)};
    transition:
        width ${SIDEBAR_TRANSITION_MS}ms ease-in-out,
        min-width ${SIDEBAR_TRANSITION_MS}ms ease-in-out;
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    display: flex;
    flex-direction: column;
    overflow: hidden;
    /* Spacing between sidebar and page comes from ContentWrapper gap (8px),
       not from sidebar margin — keeps sidebar height flush with the page. */
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
    padding: 0 12px 12px 12px;
    flex-shrink: 0;
`;

export const TreeContainer = styled.div`
    flex: 1;
    overflow-y: auto;
    overflow-x: hidden;
    padding: 8px 2px 8px 8px;
    ${hierarchicalBrowseScrollbar}
`;

export const Content = styled.div`
    display: flex;
    flex-direction: column;
    overflow: hidden;
    flex: 1;
    min-height: 0;
`;

export const HomeNavLink = styled(Link)<{ $isSelected: boolean }>`
    position: relative;
    z-index: 0;
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 4px 2px 4px 8px;
    margin: 0 0 2px 0;
    min-height: 38px;
    height: 38px;
    text-decoration: none;
    cursor: pointer;
    background: transparent;
    ${treeRowInteractionBg}
`;

export const HomeNavIcon = styled.div<{ $isSelected: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 20px;
    flex-shrink: 0;

    && svg {
        color: ${(props) => (props.$isSelected ? props.theme.colors.iconBrand : props.theme.colors.icon)};
    }
`;

export const HomeNavLabel = styled.span<{ $isSelected: boolean }>`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 14px;
    line-height: 20px;
    color: ${(props) => props.theme.colors.textSecondary};

    ${(props) =>
        props.$isSelected &&
        `
            background: ${props.theme.colors.brandGradientSelected};
            background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 600;
        `}
`;
