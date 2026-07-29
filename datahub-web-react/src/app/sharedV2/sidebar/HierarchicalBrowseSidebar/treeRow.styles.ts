import styled, { css } from 'styled-components';

import { TREE_ROW_CARET_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { getTreeRowPaddingLeft } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/utils/treeRowChrome';

/**
 * Canonical tree-row chrome for hierarchical browse sidebars.
 *
 * Tokens:
 * - Row: 38px tall; content padding matches TreeSectionHeader
 * - Hover/selected: ::before overlay (bg does not push content)
 * - Caret: 14px Phosphor, far right
 * - Icon slot: 24×20 with 8px right gutter
 */

/** Shared hover/selected paint — content stays flush; bg is a separate layer. */
export const treeRowInteractionBg = css<{ $isSelected?: boolean }>`
    &::before {
        content: '';
        position: absolute;
        inset: 0;
        border-radius: 6px;
        z-index: -1;
        pointer-events: none;
        transition:
            background-color 0.15s ease,
            box-shadow 0.15s ease;
    }

    ${(props) =>
        props.$isSelected &&
        css`
            &::before {
                background: ${props.theme.colors.bgSelectedSubtle};
                box-shadow: ${props.theme.colors.shadowFocusBrand};
            }
        `}

    ${(props) =>
        !props.$isSelected &&
        css`
            &:hover::before {
                background: ${props.theme.colors.bgHover};
                box-shadow: ${props.theme.colors.shadowFocus};
            }
        `}
`;

export const TreeRowContainer = styled.div<{
    $level: number;
    $isSelected: boolean;
    $isCollapsed?: boolean;
}>`
    position: relative;
    z-index: 0;
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.$isCollapsed ? 'center' : 'space-between')};
    padding: ${(props) => (props.$isCollapsed ? '4px 0' : `4px 2px 4px ${getTreeRowPaddingLeft(props.$level)}px`)};
    min-height: 38px;
    height: 38px;
    cursor: pointer;
    margin: 0 0 2px 0;
    background: transparent;

    ${(props) => !props.$isCollapsed && treeRowInteractionBg}
`;

export const TreeRowLeftContent = styled.div<{ $isCollapsed?: boolean }>`
    display: flex;
    align-items: center;
    ${(props) =>
        props.$isCollapsed
            ? `
        flex: 0 0 auto;
    `
            : `
        flex: 1;
        min-width: 0;
        overflow: hidden;
    `}
`;

export const TreeRowCaretSlot = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: ${TREE_ROW_CARET_SIZE + 2}px;
    height: ${TREE_ROW_CARET_SIZE + 2}px;
    flex-shrink: 0;
`;

export const TreeRowIconSlot = styled.div<{ $isCollapsed?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 20px;
    margin-right: ${(props) => (props.$isCollapsed ? '0' : '8px')};
    flex-shrink: 0;
`;

export const TreeRowTitle = styled.span<{ $isSelected: boolean }>`
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

export const TreeRowRightContent = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    margin-left: 8px;
    flex-shrink: 0;
`;

export const TreeRowExpandButton = styled.button`
    display: flex;
    align-items: center;
    justify-content: center;
    width: ${TREE_ROW_CARET_SIZE + 2}px;
    height: ${TREE_ROW_CARET_SIZE + 2}px;
    padding: 0;
    border: none;
    background: transparent;
    cursor: pointer;
    color: inherit;

    &:hover {
        opacity: 0.7;
    }
`;
