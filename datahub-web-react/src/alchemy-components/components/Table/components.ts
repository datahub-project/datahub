import styled from 'styled-components';

import { Icon } from '@components/components/Icon';

import { borders, radius, spacing, typography } from '@src/alchemy-components/theme';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';

export const TableContainer = styled.div<{ isScrollable?: boolean; maxHeight?: string; isBorderless?: boolean }>(
    ({ isScrollable, maxHeight, isBorderless, theme }) => ({
        borderRadius: isBorderless ? radius.none : radius.lg,
        border: isBorderless ? borders.none : `1px solid ${theme.colors.border}`,
        overflow: isScrollable ? 'auto' : 'hidden',
        width: '100%',
        maxHeight: maxHeight || '100%',

        '&::-webkit-scrollbar': {
            height: '8px', // For horizontal scrollbar - visible
            width: '0px', // For vertical scrollbar - invisible
        },

        '& .selected-row': {
            background: `${theme.colors.bgHover} !important`,
        },
    }),
);

// `$width` (e.g. 'max-content') lets the table grow wider than its container and scroll horizontally.
export const BaseTable = styled.table<{ $width?: string }>(({ $width }) => ({
    borderCollapse: 'collapse',
    width: $width || '100%',
    ...($width ? { minWidth: '100%' } : {}),
}));

export const TableHeader = styled.thead(({ theme }) => ({
    backgroundColor: theme.colors.bgSurface,
    borderRadius: radius.lg,
    borderBottom: `1px solid ${theme.colors.border}`,
    position: 'sticky',
    top: 0,
    zIndex: 100,
}));

export const TableHeaderCell = styled.th<{
    width?: string;
    maxWidth?: string;
    minWidth?: string;
    shouldAddRightBorder?: boolean;
}>(({ width, maxWidth, minWidth, shouldAddRightBorder, theme }) => ({
    padding: `${spacing.sm} ${spacing.md}`,
    color: theme.colors.textSecondary,
    fontSize: typography.fontSizes.sm,
    fontWeight: typography.fontWeights.medium,
    textAlign: 'start',
    width,
    maxWidth,
    minWidth,
    borderRight: shouldAddRightBorder ? `1px solid ${theme.colors.border}` : borders.none,
}));

export const HeaderContainer = styled.div<{ alignment?: AlignmentOptions }>(({ alignment }) => ({
    display: 'flex',
    alignItems: 'center',
    gap: spacing.sm,
    fontSize: '12px',
    fontWeight: 700,
    justifyContent: alignment,
}));

export const TableRow = styled.tr<{
    canExpand?: boolean;
    isRowClickable?: boolean;
    isFocused?: boolean;
    canHover?: boolean;
}>(({ canExpand, isRowClickable, isFocused, canHover, theme }) => ({
    background: canExpand ? theme.colors.bgHover : 'transparent',
    ...(isFocused
        ? {
              background: theme.colors.bgSelectedSubtle,
          }
        : {}),
    '&:hover': canHover ? { backgroundColor: theme.colors.bgHover } : {},
    cursor: isRowClickable ? 'pointer' : 'normal',
    '&:last-child': {
        '& td': {
            borderBottom: 'none',
        },
    },

    '& td:first-child': {
        fontWeight: typography.fontWeights.bold,
        color: theme.colors.text,
        fontSize: '12px',
    },
}));

export const TableCell = styled.td<{
    width?: string;
    alignment?: AlignmentOptions;
    isGroupHeader?: boolean;
    isExpanded?: boolean;
}>(({ width, alignment, isGroupHeader, theme }) => ({
    padding: isGroupHeader
        ? `${spacing.xsm} ${spacing.xsm} ${spacing.xsm} ${spacing.xsm}`
        : `${spacing.md} ${spacing.xsm} ${spacing.md} ${spacing.md}`,
    borderBottom: `1px solid ${theme.colors.border}`,
    color: theme.colors.textSecondary,
    fontSize: typography.fontSizes.md,
    fontWeight: typography.fontWeights.normal,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    maxWidth: width || 'unset',
    textAlign: (alignment as AlignmentOptions) || 'left',
}));

export const SortIconsContainer = styled.div({
    display: 'flex',
    flexDirection: 'column',
});

export const SortIcon = styled(Icon)<{ isActive?: boolean }>(({ isActive, theme }) => ({
    margin: '-3px',
    stroke: isActive ? theme.colors.borderBrand : undefined,

    ':hover': {
        cursor: 'pointer',
    },
}));

export const LoadingContainer = styled.div(({ theme }) => ({
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    height: '100%',
    width: '100%',
    gap: spacing.sm,
    color: theme.colors.buttonFillBrand,
    fontSize: typography.fontSizes['3xl'],
}));

export const CheckboxWrapper = styled.div({
    display: 'flex',
    alignItems: 'center',
});

export const HorizontalScrollContainer = styled(TableContainer)`
    overflow-x: auto;
`;

export const FixedTableHeaderCell = styled(TableHeaderCell)<{ $left: string }>`
    position: sticky;
    left: ${({ $left }) => $left};
    z-index: 2;
    background-color: ${({ theme }) => theme.colors.bgSurface};
`;

// Sticky cells need an opaque background, so they mirror the row's hover/focus/group backgrounds.
export const FixedTableCell = styled(TableCell)<{ $left: string; $isFocused?: boolean; $isGroupRow?: boolean }>`
    position: sticky;
    left: ${({ $left }) => $left};
    z-index: 1;
    background-color: ${({ theme, $isFocused, $isGroupRow }) => {
        if ($isFocused) return theme.colors.bgSelectedSubtle;
        if ($isGroupRow) return theme.colors.bgHover;
        return theme.colors.bg;
    }};

    tr:hover > &,
    .selected-row > & {
        background-color: ${({ theme }) => theme.colors.bgHover};
    }
`;

export const TreeCellContent = styled.div<{ $indent: number }>`
    display: flex;
    align-items: center;
    gap: 4px;
    min-width: 0;
    padding-left: ${({ $indent }) => $indent}px;
`;

export const TreeExpandButton = styled.button`
    display: inline-flex;
    flex-shrink: 0;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    padding: 0;
    border: none;
    border-radius: 4px;
    background: transparent;
    color: ${({ theme }) => theme.colors.icon};
    cursor: pointer;

    &:hover {
        background-color: ${({ theme }) => theme.colors.bgHover};
    }
`;

export const TreeExpandSpacer = styled.span`
    display: inline-block;
    flex-shrink: 0;
    width: 20px;
`;

export const EllipsisContent = styled.div`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    min-width: 0;
`;

export const EmptyStateCell = styled.td`
    padding: 24px 16px;
    text-align: center;
    color: ${({ theme }) => theme.colors.textSecondary};
`;
