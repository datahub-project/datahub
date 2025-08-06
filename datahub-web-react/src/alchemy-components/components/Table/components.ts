import { Icon } from '@components';
import styled from 'styled-components';

import { borders, colors, radius, spacing, typography } from '@src/alchemy-components/theme';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';

export const TableContainer = styled.div<{ isScrollable?: boolean; maxHeight?: string; isBorderless?: boolean }>(
    ({ isScrollable, maxHeight, isBorderless }) => ({
        borderRadius: isBorderless ? radius.none : radius.lg,
        border: isBorderless ? borders.none : `1px solid ${colors.gray[1400]}`,
        overflow: isScrollable ? 'auto' : 'hidden',
        width: '100%',
        maxHeight: maxHeight || '100%',

        '&::-webkit-scrollbar': {
            height: '8px', // For horizontal scrollbar - visible
            width: '0px', // For vertical scrollbar - invisible
        },

        '& .selected-row': {
            background: `${colors.gray[100]} !important`,
        },
    }),
);

export const BaseTable = styled.table({
    borderCollapse: 'collapse',
    width: '100%',
});

export const TableHeader = styled.thead({
    backgroundColor: colors.gray[1500],
    boxShadow: '0px 1px 1px rgba(0, 0, 0, 0.1)',
    borderRadius: radius.lg,
    borderBottom: `1px solid ${colors.gray[1400]}`,
    position: 'sticky',
    top: 0,
    zIndex: 100,
});

export const TableHeaderCell = styled.th<{
    width?: string;
    maxWidth?: string;
    minWidth?: string;
    shouldAddRightBorder?: boolean;
}>(({ width, maxWidth, minWidth, shouldAddRightBorder }) => ({
    padding: `${spacing.sm} ${spacing.md}`,
    color: colors.gray[1700],
    fontSize: typography.fontSizes.sm,
    fontWeight: typography.fontWeights.medium,
    textAlign: 'start',
    width: width || 'auto',
    maxWidth,
    minWidth,
    borderRight: shouldAddRightBorder ? `1px solid ${colors.gray[1400]}` : borders.none,
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
}>(({ canExpand, isRowClickable, isFocused, canHover }) => ({
    background: canExpand ? colors.gray[100] : 'transparent',
    ...(isFocused
        ? {
              background: `linear-gradient(180deg, rgba(83,63,209,0.04) -3.99%, rgba(112,94,228,0.04) 53.04%, rgba(112,94,228,0.04) 100%)`,
          }
        : {}),
    '&:hover': canHover ? { backgroundColor: colors.gray[1500] } : {},
    cursor: isRowClickable ? 'pointer' : 'normal',
    '&:last-child': {
        '& td': {
            borderBottom: 'none',
        },
    },

    '& td:first-child': {
        fontWeight: typography.fontWeights.bold,
        color: colors.gray[600],
        fontSize: '12px',
    },
}));

export const TableCell = styled.td<{
    width?: string;
    alignment?: AlignmentOptions;
    isGroupHeader?: boolean;
    isExpanded?: boolean;
}>(({ width, alignment, isGroupHeader, isExpanded }) => ({
    padding: isGroupHeader
        ? `${spacing.xsm} ${spacing.xsm} ${spacing.xsm} ${spacing.md}`
        : `${spacing.md} ${spacing.xsm} ${spacing.md} ${spacing.md}`,
    borderBottom: isGroupHeader && !isExpanded ? `1px solid ${colors.gray[200]}` : `1px solid ${colors.gray[100]}`,
    color: colors.gray[1700],
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

export const SortIcon = styled(Icon)<{ isActive?: boolean }>(({ isActive }) => ({
    margin: '-3px',
    stroke: isActive ? colors.violet[600] : undefined,

    ':hover': {
        cursor: 'pointer',
    },
}));

export const LoadingContainer = styled.div({
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    height: '100%',
    width: '100%',
    gap: spacing.sm,
    color: colors.violet[700],
    fontSize: typography.fontSizes['3xl'],
});

export const CheckboxWrapper = styled.div({
    display: 'flex',
    alignItems: 'center',
});
