import styled from 'styled-components';
import { spacing, colors, shadows, transition, radius } from '@components/theme';
import { Button, Icon } from '@components';
import { SearchInputProps, SelectSizeOptions, SelectStyleProps } from './types';
import { getSelectStyle, getOptionLabelStyle, getSelectFontStyles, getSelectPadding } from './utils';

/**
 * Base Select component styling
 */
export const SelectBase = styled.div<SelectStyleProps>(({ isDisabled, isReadOnly, fontSize }) => ({
    ...getSelectStyle({ isDisabled, isReadOnly, fontSize }),
    display: 'flex',
    flexDirection: 'row' as const,
    gap: spacing.xsm,
    transition: transition.easing['ease-in'],
    justifyContent: 'space-between',
    alignItems: 'center',
}));

/**
 * Styled components specific to the Basic version of the Select component
 */

// Container for the Basic Select component
interface ContainerProps {
    size: SelectSizeOptions;
    width?: number | 'full';
}

export const Container = styled.div<ContainerProps>(({ size, width }) => ({
    position: 'relative',
    display: 'flex',
    flexDirection: 'column',
    width: width === 'full' ? '100%' : `${width}px`,
    gap: '4px',
    transition: transition.easing['ease-in'],
    minWidth: '175px',
    ...getSelectFontStyles(size),
}));

export const Dropdown = styled.div({
    position: 'absolute',
    top: '100%',
    left: 0,
    right: 0,
    borderRadius: radius.lg,
    background: colors.white,
    zIndex: 1,
    transition: transition.easing['ease-in-out'],
    boxShadow: shadows.dropdown,
    padding: spacing.xsm,
    display: 'flex',
    flexDirection: 'column',
    gap: '8px',
    marginTop: '4px',
});

export const SearchInputContainer = styled.div({
    position: 'relative',
    width: '100%',
    display: 'flex',
    justifyContent: 'center',
});

export const SearchInput = styled.input<SearchInputProps>(({ fontSize }) => ({
    ...getSelectPadding(fontSize),
    width: '90%',
    borderRadius: radius.md,
    border: `1px solid ${colors.gray[100]}`,
    '&:focus': {
        border: `1px solid ${colors.violet[600]}`,
        outline: 'none',
    },
    '&::placeholder': {
        color: colors.gray[400],
    },
    color: colors.gray[500],
    paddingRight: spacing.xlg,
}));

export const SearchIcon = styled(Icon)({
    position: 'absolute',
    right: spacing.sm,
    top: '50%',
    transform: 'translateY(-50%)',
    pointerEvents: 'none',
});

// Styled components for SelectValue (Selected value display)
export const SelectValue = styled.span({
    fontWeight: 500,
    color: colors.gray[400],
});

export const Placeholder = styled.span({
    fontWeight: 500,
    color: colors.gray[300],
});

export const ActionButtonsContainer = styled.div({
    display: 'flex',
    gap: '6px',
    flexDirection: 'row',
    alignItems: 'center',
});

/**
 * Components that can be reused to create new Select variants
 */

export const FooterBase = styled.div({
    display: 'flex',
    justifyContent: 'flex-end',
    gap: spacing.sm,
    paddingTop: spacing.sm,
    borderTop: `1px solid ${colors.gray[50]}`,
});

export const OptionList = styled.div({
    display: 'flex',
    flexDirection: 'column' as const,
});

export const OptionLabel = styled.label<{ isSelected: boolean }>(({ isSelected }) => ({
    ...getOptionLabelStyle(isSelected),
}));

export const SelectLabel = styled.label({
    color: colors.gray[600],
    fontWeight: 500,
});

export const StyledCancelButton = styled(Button)({
    backgroundColor: colors.violet[25],
    color: colors.violet[500],
    borderColor: colors.violet[25],
    '&:hover': {
        backgroundColor: colors.violet[50],
        color: colors.violet[700],
        borderColor: colors.violet[50],
    },
});

export const StyledClearButton = styled(Button)({
    backgroundColor: colors.gray[200],
    border: `1px solid ${colors.gray[200]}`,
    color: colors.black,
    padding: '1px',
    position: 'absolute',
    right: '35px',
    '&:hover': {
        backgroundColor: colors.violet[50],
        color: colors.violet[700],
        borderColor: colors.violet[50],
        boxShadow: shadows.none,
    },
    '&:focus': {
        backgroundColor: colors.violet[50],
        color: colors.violet[700],
        boxShadow: `0 0 0 2px ${colors.white}, 0 0 0 4px ${colors.violet[50]}`,
    },
});

export const ClearIcon = styled.span({
    cursor: 'pointer',
    marginLeft: '8px',
});

export const ArrowIcon = styled.span<{ isOpen: boolean }>(({ isOpen }) => ({
    marginLeft: 'auto',
    border: 'solid black',
    borderWidth: '0 1px 1px 0',
    display: 'inline-block',
    padding: '3px',
    transform: isOpen ? 'rotate(-135deg)' : 'rotate(45deg)',
}));
