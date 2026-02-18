import { Icon } from '@components';
import styled from 'styled-components';

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
            height: '8px',
            width: '0px',
        },

        '& .selected-row': {
            background: `${theme.colors.bgHover} !important`,
        },
    }),
);

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
    width: width || 'auto',
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
              background: `linear-gradient(180deg, rgba(83,63,209,0.04) -3.99%, rgba(112,94,228,0.04) 53.04%, rgba(112,94,228,0.04) 100%)`,
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
        ? `${spacing.xsm} ${spacing.xsm} ${spacing.xsm} ${spacing.md}`
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
