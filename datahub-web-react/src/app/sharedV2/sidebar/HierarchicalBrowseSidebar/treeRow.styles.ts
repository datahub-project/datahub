import styled, { css } from 'styled-components';

import { TREE_ROW_CARET_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { getTreeRowPaddingLeft } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/utils/treeRowChrome';

/** Shared row height / gap for tree, home, and collapsed rail. */
export const treeRowHitTarget = css`
    position: relative;
    z-index: 0;
    min-height: 38px;
    height: 38px;
    margin: 0 0 2px 0;
    background: transparent;
`;

/** Hover/selected paint via ::before so padding doesn’t shift content. */
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
        props.$isSelected
            ? css`
                  &::before {
                      background: ${props.theme.colors.bgSelectedSubtle};
                      box-shadow: ${props.theme.colors.shadowFocusBrand};
                  }
              `
            : css`
                  &:hover::before {
                      background: ${props.theme.colors.bgHover};
                      box-shadow: ${props.theme.colors.shadowFocus};
                  }
              `}
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

/**
 * Notion-style expand control: entity icon at rest, caret on row hover or when
 * expanded. CSS-only swap avoids React hover state (which flickered on large trees).
 */
export const TreeRowLeadingExpand = styled.button<{ $isExpanded: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 20px;
    padding: 0;
    border: none;
    background: transparent;
    cursor: pointer;
    color: ${(props) => props.theme.colors.icon};
    flex-shrink: 0;

    .tree-row-entity-icon {
        display: ${(props) => (props.$isExpanded ? 'none' : 'flex')};
        align-items: center;
        justify-content: center;
    }

    .tree-row-caret-icon {
        display: ${(props) => (props.$isExpanded ? 'flex' : 'none')};
        align-items: center;
        justify-content: center;
    }

    &:hover {
        opacity: 0.7;
    }

    &:disabled {
        cursor: default;
        opacity: 0.5;
    }
`;

export const TreeRowRightContent = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    margin-left: 8px;
    flex-shrink: 0;
`;

/** Child count — hidden at rest, shown on row hover. */
export const TreeRowCount = styled.div`
    display: none;
    align-items: center;
    flex-shrink: 0;
`;

/**
 * Row actions (Documents ⋮ / +) — always mounted, CSS-revealed on hover so large
 * trees don’t pay React setState per row. Pin with --pinned while a menu or dialog
 * is open (mouse may leave the row into a portal).
 */
export const TREE_ROW_HOVER_ACTIONS_CLASS = 'tree-row-hover-actions';
export const TREE_ROW_HOVER_ACTIONS_PINNED_CLASS = 'tree-row-hover-actions--pinned';

/** Row hover: reveal leading caret, child count, and optional actions. */
export const treeRowHoverChrome = css`
    &:hover ${TreeRowLeadingExpand}:not([aria-expanded='true']),
    &:focus-within ${TreeRowLeadingExpand}:not([aria-expanded='true']) {
        .tree-row-entity-icon {
            display: none;
        }

        .tree-row-caret-icon {
            display: flex;
        }
    }

    &:hover
        ${TreeRowCount},
        &:focus-within
        ${TreeRowCount},
        &:has(.${TREE_ROW_HOVER_ACTIONS_PINNED_CLASS})
        ${TreeRowCount} {
        display: flex;
    }

    .${TREE_ROW_HOVER_ACTIONS_CLASS} {
        display: none;
        align-items: center;
        gap: 4px;
    }

    &:hover
        .${TREE_ROW_HOVER_ACTIONS_CLASS},
        &:focus-within
        .${TREE_ROW_HOVER_ACTIONS_CLASS},
        .${TREE_ROW_HOVER_ACTIONS_PINNED_CLASS} {
        display: flex;
    }

    /* Collapse right column when it only holds hover-revealed chrome. */
    &:not(:hover):not(:focus-within):not(:has(.${TREE_ROW_HOVER_ACTIONS_PINNED_CLASS}))
        ${TreeRowRightContent}:not(:has(> :not(${TreeRowCount}):not(.${TREE_ROW_HOVER_ACTIONS_CLASS}))) {
        display: none;
    }
`;

export const TreeRowContainer = styled.div<{
    $isSelected: boolean;
    $isCollapsed?: boolean;
}>`
    ${treeRowHitTarget}
    display: flex;
    align-items: center;
    justify-content: ${(props) => (props.$isCollapsed ? 'center' : 'space-between')};
    /* Level indent on ExpandZone; match right inset so selected rows aren’t lopsided. */
    padding: ${(props) => (props.$isCollapsed ? '4px 0' : '4px 8px 4px 0')};
    cursor: pointer;

    ${(props) => !props.$isCollapsed && treeRowInteractionBg}
    ${(props) => !props.$isCollapsed && treeRowHoverChrome}
`;

/**
 * Indent + icon: expand/collapse tap target for parents. Title stays outside so
 * it remains navigation. Leaves let clicks bubble to the row.
 */
export const TreeRowExpandZone = styled.div<{ $level: number; $expandable: boolean }>`
    display: flex;
    align-items: center;
    align-self: stretch;
    padding-left: ${(props) => getTreeRowPaddingLeft(props.$level)}px;
    flex-shrink: 0;
    cursor: ${(props) => (props.$expandable ? 'pointer' : 'inherit')};
`;

export const TreeRowLeftContent = styled.div<{ $isCollapsed?: boolean }>`
    display: flex;
    align-items: center;
    ${(props) =>
        props.$isCollapsed
            ? `flex: 0 0 auto;`
            : `
        flex: 1;
        min-width: 0;
        overflow: hidden;
    `}
`;

/** Shared by tree rows and home nav. */
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

/** Section expand-all / header chevron hit target. */
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
    color: ${(props) => props.theme.colors.icon};
    flex-shrink: 0;

    &:hover {
        opacity: 0.7;
    }

    &:disabled {
        cursor: default;
        opacity: 0.5;
    }
`;
