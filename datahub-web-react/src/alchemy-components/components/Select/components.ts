import { Button, Icon } from '@components';
import { Checkbox } from 'antd';
import styled from 'styled-components';

import { SelectLabelVariants, SelectSizeOptions, SelectStyleProps } from '@components/components/Select/types';
import { getOptionLabelStyle, getSelectFontStyles, getSelectStyle } from '@components/components/Select/utils';
import {
    formLabelTextStyles,
    inputPlaceholderTextStyles,
    inputValueTextStyles,
} from '@components/components/commonStyles';
import { colors, radius, shadows, spacing, transition, typography, zIndices } from '@components/theme';

const sharedTransition = `${transition.property.colors} ${transition.easing['ease-in-out']} ${transition.duration.normal}`;

/**
 * Base Select component styling
 */
export const SelectBase = styled.div<SelectStyleProps>(
    ({ isDisabled, isReadOnly, fontSize, isOpen, width, position }) => ({
        ...getSelectStyle({ isDisabled, isReadOnly, fontSize, isOpen }),
        display: 'flex',
        flexDirection: 'row' as const,
        gap: spacing.xsm,
        transition: sharedTransition,
        justifyContent: 'space-between',
        alignSelf: position || 'end',
        minHeight: '36px',
        alignItems: 'center',
        overflow: 'auto',
        textWrapMode: 'nowrap',
        backgroundColor: isDisabled ? colors.gray[1500] : colors.white,
        width: width === 'full' ? '100%' : 'max-content',
    }),
);

export const SelectLabelContainer = styled.div({
    display: 'flex',
    flexDirection: 'row' as const,
    gap: spacing.xsm,
    lineHeight: typography.lineHeights.none,
    alignItems: 'center',
    maxWidth: 'calc(100% - 54px)',
});

/**
 * Styled components specific to the Basic version of the Select component
 */

// Container for the Basic Select component
interface ContainerProps {
    size: SelectSizeOptions;
    width?: number | 'full' | 'fit-content';
    $selectLabelVariant?: SelectLabelVariants;
    isSelected?: boolean;
}

export const Container = styled.div<ContainerProps>(({ size, width, $selectLabelVariant, isSelected }) => {
    const getMinWidth = () => {
        if (width === 'fit-content') return 'undefined';
        if ($selectLabelVariant === 'labeled') {
            return isSelected ? '145px' : '103px';
        }
        return '175px';
    };

    const getWitdh = () => {
        switch (width) {
            case 'full':
                return '100%';
            case 'fit-content':
                return 'fit-content';
            default:
                return `${width}px`;
        }
    };

    return {
        position: 'relative',
        display: 'flex',
        flexDirection: 'column',
        width: getWitdh(),
        gap: '4px',
        transition: sharedTransition,
        minWidth: getMinWidth(),
        ...getSelectFontStyles(size),
        ...inputValueTextStyles(size),
    };
});

export const DropdownContainer = styled.div<{ ignoreMaxHeight?: boolean }>(({ ignoreMaxHeight }) => ({
    borderRadius: radius.md,
    background: colors.white,
    zIndex: zIndices.dropdown,
    transition: sharedTransition,
    boxShadow: shadows.dropdown,
    padding: spacing.xsm,
    display: 'flex',
    flexDirection: 'column',
    gap: '8px',
    marginTop: '4px',
    overflow: 'auto',
    width: '100%',
    maxHeight: ignoreMaxHeight ? undefined : '360px',
}));

// Styled components for SelectValue (Selected value display)
export const SelectValue = styled.span({
    ...inputValueTextStyles(),
    color: colors.gray[600],
});

export const Placeholder = styled.span({
    ...inputPlaceholderTextStyles,
    color: colors.gray[1800],
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

export const OptionList = styled.div({
    display: 'flex',
    flexDirection: 'column' as const,
    overflow: 'auto',
});

export const LabelContainer = styled.div({
    display: 'flex',
    justifyContent: 'space-between',
    width: '100%',
    alignItems: 'center',
    gap: '12px',
});

export const OptionContainer = styled.div({
    display: 'flex',
    flexDirection: 'column',
});

export const DescriptionContainer = styled.span({
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    width: '100%',
    color: colors.gray[500],
    lineHeight: 'normal',
    fontSize: typography.fontSizes.sm,
    marginTop: spacing.xxsm,
});

export const LabelsWrapper = styled.div<{ shouldShowGap?: boolean }>(({ shouldShowGap = false }) => ({
    display: 'flex',
    flexWrap: 'wrap',
    gap: shouldShowGap ? spacing.xxsm : '0px',
    maxHeight: '150px',
    maxWidth: '100%',
}));

export const OptionLabel = styled.label<{
    isSelected: boolean;
    isMultiSelect?: boolean;
    isDisabled?: boolean;
    applyHoverWidth?: boolean;
}>(({ isSelected, isMultiSelect, isDisabled, applyHoverWidth }) => ({
    ...getOptionLabelStyle(isSelected, isMultiSelect, isDisabled, applyHoverWidth),
}));

export const SelectLabel = styled.label({
    ...formLabelTextStyles,
    marginBottom: spacing.xxsm,
    textAlign: 'left',
});

export const StyledIcon = styled(Icon)({
    flexShrink: 0,
    color: colors.gray[1800],
});

export const StyledClearButton = styled(Button).attrs({
    variant: 'text',
})({
    color: colors.gray[1800],
    padding: '0px',

    '&:hover': {
        border: 'none',
        backgroundColor: colors.transparent,
        borderColor: colors.transparent,
        boxShadow: shadows.none,
    },

    '&:focus': {
        border: 'none',
        backgroundColor: colors.transparent,
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

export const StyledCheckbox = styled(Checkbox)({
    '.ant-checkbox-checked:not(.ant-checkbox-disabled) .ant-checkbox-inner': {
        backgroundColor: colors.violet[500],
        borderColor: `${colors.violet[500]} !important`,
    },
});

export const StyledBubbleButton = styled(Button)({
    backgroundColor: colors.gray[200],
    border: `1px solid ${colors.gray[200]}`,
    color: colors.black,
    padding: '1px',
});

export const HighlightedLabel = styled.span`
    background-color: ${colors.gray[100]};
    padding: 4px 6px;
    border-radius: 8px;
    font-size: ${typography.fontSizes.sm};
    color: ${colors.gray[500]};
`;
