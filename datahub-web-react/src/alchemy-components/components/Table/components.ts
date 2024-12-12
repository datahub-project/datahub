import { Icon } from '@components';
import { colors, radius, spacing, typography, borders } from '@src/alchemy-components/theme';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import styled from 'styled-components';

export const TableContainer = styled.div<{ isScrollable?: boolean; maxHeight?: string; isBorderless?: boolean }>(
    ({ isScrollable, maxHeight, isBorderless }) => ({
        borderRadius: isBorderless ? radius.none : radius.lg,
        border: isBorderless ? borders.none : `1px solid ${colors.gray[1400]}`,
        overflow: 'hidden',
        width: '100%',
        maxHeight: maxHeight || '100%',
    }),
);

export const BaseTable = styled.table({
    borderCollapse: 'collapse',
    width: '100%',
});

export const TableHeader = styled.thead({
    backgroundColor: colors.gray[1500],
    borderRadius: radius.lg,
    position: 'sticky',
    top: 0,
    zIndex: 100,
});

export const TableHeaderCell = styled.th<{ width?: string; shouldAddRightBorder?: boolean }>(
    ({ width, shouldAddRightBorder }) => ({
        padding: `${spacing.sm} ${spacing.xsm}`,
        color: colors.gray[600],
        fontSize: typography.fontSizes.sm,
        fontWeight: typography.fontWeights.medium,
        textAlign: 'start',
        width: width || 'auto',
        borderRight: shouldAddRightBorder ? `1px solid ${colors.gray[1400]}` : borders.none,
    }),
);

export const HeaderContainer = styled.div({
    display: 'flex',
    alignItems: 'center',
    gap: spacing.sm,
    fontSize: '12px',
    fontWeight: 700,
});

export const TableRow = styled.tr<{ canExpand?: boolean }>(({ canExpand }) => ({
    background: canExpand ? '#EBECF0' : 'transparent',
    cursor: 'pointer',
    '&:last-child': {
        '& td': {
            borderBottom: 'none',
        },
    },

    '& td:first-child': {
        fontWeight: typography.fontWeights.medium,
        color: colors.gray[600],
    },
}));

export const TableCell = styled.td<{
    width?: string;
    alignment?: AlignmentOptions;
    shouldUseMinimumPadding?: boolean;
    isExpanded?: boolean;
}>(({ width, alignment, shouldUseMinimumPadding, isExpanded }) => ({
    padding: shouldUseMinimumPadding ? spacing.xsm : `${spacing.md} ${spacing.xsm}`,
    borderBottom: isExpanded ? `1px solid ${colors.gray[200]}` : `1px solid ${colors.gray[100]}`,
    color: colors.gray[1700],
    fontSize: typography.fontSizes.md,
    fontWeight: typography.fontWeights.normal,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'normal',
    wordBreak: 'break-word',
    maxWidth: width || 'unset',
    textAlign: alignment || 'left',
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
