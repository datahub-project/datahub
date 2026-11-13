import styled from 'styled-components';

import { Button } from '@components/components/Button';
import { Icon } from '@components/components/Icon';

import { borders, radius, spacing, typography } from '@src/alchemy-components/theme';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';

const getOpaqueTint = (tint: string, base: string) => `linear-gradient(${tint}, ${tint}), ${base}`;

export const TableContainer = styled.div<{
    isScrollable?: boolean;
    maxHeight?: string;
    isBorderless?: boolean;
    isVirtualized?: boolean;
}>(({ isScrollable, maxHeight, isBorderless, isVirtualized, theme }) => ({
    borderRadius: isBorderless ? radius.none : radius.lg,
    border: isBorderless ? borders.none : `1px solid ${theme.colors.border}`,
    overflow: isScrollable ? 'auto' : 'hidden',
    width: '100%',
    maxHeight: maxHeight || '100%',
    // A virtualized body swaps rows for spacers as it scrolls. Scroll anchoring would answer each swap by
    // nudging scrollTop, which moves the window again: rows at the edge mount and unmount in a loop.
    overflowAnchor: isVirtualized ? 'none' : undefined,

    '&::-webkit-scrollbar': {
        height: '8px', // For horizontal scrollbar - visible
        width: '0px', // For vertical scrollbar - invisible
    },

    '& .selected-row': {
        background: `${theme.colors.bgHover} !important`,
    },

    '& .selected-row > td[data-fixed]': {
        background: `${getOpaqueTint(theme.colors.bgHover, theme.colors.bg)} !important`,
    },
}));

export const BaseTable = styled.table({
    borderCollapse: 'collapse',
    width: '100%',
});

export const TableHeader = styled.thead(({ theme }) => ({
    backgroundColor: theme.colors.bgSurface,
    borderRadius: radius.lg,
    borderBottom: `1px solid ${theme.colors.border}`,
    position: 'sticky',
    top: 0,
    zIndex: 100,
}));

// Pinned cells need an opaque background so scrolled content doesn't show through them.
const getFixedCellStyles = (fixedLeft: number | undefined, background: string, zIndex: number) =>
    fixedLeft !== undefined ? { position: 'sticky' as const, left: fixedLeft, zIndex, background } : {};

export const TableHeaderCell = styled.th<{
    width?: string;
    maxWidth?: string;
    minWidth?: string;
    shouldAddRightBorder?: boolean;
    fixedLeft?: number;
    /** Wider left padding, for the first column of a grouped table so group carets have room. */
    gutter?: string;
    size?: 'sm' | 'md';
}>(({ width, maxWidth, minWidth, shouldAddRightBorder, fixedLeft, gutter, size, theme }) => ({
    padding: size === 'sm' ? spacing.xsm : `${spacing.sm} ${spacing.md}`,
    paddingLeft: gutter,
    color: theme.colors.textSecondary,
    fontSize: typography.fontSizes.sm,
    fontWeight: typography.fontWeights.medium,
    textAlign: 'start',
    width,
    maxWidth,
    minWidth,
    borderRight: shouldAddRightBorder ? `1px solid ${theme.colors.border}` : borders.none,
    ...getFixedCellStyles(fixedLeft, theme.colors.bgSurface, 1),
}));

export const HeaderContainer = styled.div<{ alignment?: AlignmentOptions }>(({ alignment }) => ({
    display: 'flex',
    alignItems: 'center',
    gap: spacing.sm,
    fontSize: typography.fontSizes.sm,
    fontWeight: typography.fontWeights.bold,
    justifyContent: alignment,
}));

export const TableRow = styled.tr<{
    isRowClickable?: boolean;
    isFocused?: boolean;
    canHover?: boolean;
}>(({ isRowClickable, isFocused, canHover, theme }) => ({
    background: 'transparent',
    ...(isFocused
        ? {
              background: theme.colors.bgSelectedSubtle,
          }
        : {}),
    '&:hover': canHover ? { backgroundColor: theme.colors.bgHover } : {},
    // Row tints are translucent: pinned cells paint the tint once over an opaque base so they match
    // the rest of the row and hide the cells scrolling underneath.
    '& > td[data-fixed]': {
        background: isFocused ? getOpaqueTint(theme.colors.bgSelectedSubtle, theme.colors.bg) : theme.colors.bg,
    },
    '&:hover > td[data-fixed]': canHover ? { background: getOpaqueTint(theme.colors.bgHover, theme.colors.bg) } : {},
    cursor: isRowClickable ? 'pointer' : 'normal',
    '&:last-child': {
        '& td': {
            borderBottom: 'none',
        },
    },
}));

export const TableCell = styled.td<{
    width?: string;
    alignment?: AlignmentOptions;
    fixedLeft?: number;
    gutter?: string;
    /** The row's first data column (after any checkbox column), shown as the row's title. */
    isPrimary?: boolean;
    /** Virtualized rows have a fixed height, so cells drop their vertical padding and center content. */
    isFixedHeight?: boolean;
    size?: 'sm' | 'md';
}>(({ width, alignment, fixedLeft, gutter, isPrimary, isFixedHeight, size, theme }) => {
    const verticalPadding = isFixedHeight ? '0' : (size === 'sm' && spacing.xsm) || spacing.md;
    const leftPadding = size === 'sm' ? spacing.xsm : spacing.md;
    return {
        padding: `${verticalPadding} ${spacing.xsm} ${verticalPadding} ${leftPadding}`,
        paddingLeft: gutter,
        borderBottom: `1px solid ${theme.colors.border}`,
        color: theme.colors.textSecondary,
        fontSize: typography.fontSizes.md,
        fontWeight: typography.fontWeights.normal,
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
        maxWidth: width || 'unset',
        textAlign: (alignment as AlignmentOptions) || 'left',
        ...(isPrimary
            ? {
                  color: theme.colors.text,
                  fontSize: typography.fontSizes.sm,
                  fontWeight: typography.fontWeights.bold,
              }
            : {}),
        ...getFixedCellStyles(fixedLeft, theme.colors.bg, 1),
    };
});

export const SortIconsContainer = styled.div({
    display: 'flex',
    flexDirection: 'column',
});

export const SortIcon = styled(Icon)({
    margin: '-3px',

    ':hover': {
        cursor: 'pointer',
    },
});

export const LoadingContainer = styled.div(({ theme }) => ({
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    height: '100%',
    width: '100%',
    gap: spacing.sm,
    color: theme.colors.textSecondary,
    fontSize: typography.fontSizes['3xl'],
}));

export const CheckboxWrapper = styled.div({
    display: 'flex',
    alignItems: 'center',
});

export const GroupHeaderRow = styled.tr<{ rowHeight?: number }>(({ rowHeight, theme }) => ({
    height: rowHeight ?? 40,
    cursor: 'pointer',
    '& > td': {
        backgroundColor: theme.colors.bgSurface,
    },
    '&:hover > td': {
        backgroundColor: theme.colors.bgHover,
    },
}));

export const GroupHeaderCell = styled.td<{ isSticky?: boolean; stickyTop?: number; gutter?: string }>(
    ({ isSticky, stickyTop, gutter, theme }) => ({
        padding: `${spacing.xxsm} ${spacing.md}`,
        paddingLeft: gutter,
        borderBottom: `1px solid ${theme.colors.border}`,
        ...(isSticky
            ? {
                  position: 'sticky',
                  top: stickyTop ?? 0,
                  // Below the column header (zIndex 100) so it slides under it, above regular and pinned cells.
                  zIndex: 2,
              }
            : {}),
    }),
);

export const GroupHeaderInner = styled.div({
    display: 'flex',
    alignItems: 'center',
    gap: spacing.xsm,
    minWidth: 0,
});

// Starts at the cell's content edge so the icon and label line up with the column text below; the
// caret hangs in the cell padding to the left, like Linear's group rows.
export const GroupHeaderContent = styled.div<{ gutter?: string }>(({ gutter }) => ({
    display: 'flex',
    alignItems: 'center',
    gap: spacing.xsm,
    minWidth: 0,
    // Keeps the label in view when a wide table scrolls horizontally.
    position: 'sticky',
    left: gutter ?? spacing.md,
}));

export const GroupCaretSlot = styled.span({
    position: 'absolute',
    right: '100%',
    top: '50%',
    transform: 'translateY(-50%)',
    paddingRight: spacing.xxsm,
    display: 'flex',
});

export const GroupCaretButton = styled(Button)({
    '&&': {
        padding: spacing.xxsm,
    },
});

export const GroupHeaderLabel = styled.div(({ theme }) => ({
    display: 'flex',
    alignItems: 'center',
    gap: spacing.xsm,
    minWidth: 0,
    color: theme.colors.text,
    fontSize: typography.fontSizes.sm,
    fontWeight: typography.fontWeights.bold,
}));

export const GroupHeaderActions = styled.div({
    display: 'flex',
    alignItems: 'center',
    gap: spacing.xsm,
    marginLeft: 'auto',
    position: 'sticky',
    right: spacing.md,
});

export const TreeCellContent = styled.div<{ indent: number }>(({ indent }) => ({
    display: 'flex',
    alignItems: 'center',
    gap: spacing.xxsm,
    paddingLeft: indent,
    minWidth: 0,
}));

// Same box with or without a caret, so leaf rows line up with their expandable siblings. The fixed
// height lets the caret button overflow instead of making expandable rows taller than leaf rows.
export const TreeExpanderSlot = styled.span({
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    flexShrink: 0,
    width: '24px',
    height: '16px',
});

// Holds a virtualized cell at the row height: a taller cell would grow its row past `rowHeight`, and the
// spacers that stand in for off-screen rows would no longer match the rows they replace. It fills the row
// (less the cell's 1px bottom border) and centers its content, so content that draws a little outside
// itself, like a badge's count, stays inside the clip.
export const FixedHeightCellContent = styled.div<{ rowHeight: number }>(({ rowHeight }) => ({
    height: rowHeight - 1,
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'center',
    overflow: 'hidden',
}));

export const SpacerCell = styled.td({
    padding: 0,
    border: 'none',
});

export const RowDetailCell = styled(TableCell)({
    padding: 0,
});

export const RowDetailContent = styled.div({
    display: 'flex',
    flexDirection: 'column',
    width: '100%',
    minWidth: 0,
});
