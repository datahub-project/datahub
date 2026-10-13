import styled from 'styled-components';

import { Icon } from '@components/components/Icon';
import { SelectLabelVariants, SelectSizeOptions, SelectStyleProps } from '@components/components/Select/types';
import {
    getDropdownStyle,
    getOptionLabelStyle,
    getSelectFontStyles,
    getSelectStyle,
} from '@components/components/Select/utils';
import {
    formLabelTextStyles,
    inputPlaceholderTextStyles,
    inputValueTextStyles,
} from '@components/components/commonStyles';
import { radius, spacing, transition, typography, zIndices } from '@components/theme';

const sharedTransition = `${transition.property.colors} ${transition.easing['ease-in-out']} ${transition.duration.normal}`;

/**
 * Base Select component styling
 */
export const SelectBase = styled.div<SelectStyleProps>(
    ({ isDisabled, isReadOnly, fontSize, isOpen, width, maxWidth, position, theme }) => ({
        ...getSelectStyle({ isDisabled, isReadOnly, fontSize, isOpen, theme }),
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
        width: width === 'full' ? '100%' : 'max-content',
        maxWidth: maxWidth ? `${maxWidth}px` : undefined,
    }),
);

export const SelectLabelContainer = styled.div({
    display: 'flex',
    flexDirection: 'row' as const,
    gap: spacing.xsm,
    // `none` (1) clips descenders under overflow:hidden on SelectValue (e.g. "g" in Engineering).
    lineHeight: typography.lineHeights.sm,
    alignItems: 'center',
    maxWidth: 'calc(100% - 10px)',
    // Lets this shrink below its content's natural width so a capped SelectBase
    // (via the `maxWidth` prop) can actually truncate the label instead of overflowing.
    minWidth: 0,
});

/**
 * Styled components specific to the Basic version of the Select component
 */

// Container for the Basic Select component
interface ContainerProps {
    size: SelectSizeOptions;
    width?: number | 'full' | 'fit-content';
    $minWidth?: string;
    $selectLabelVariant?: SelectLabelVariants;
    isSelected?: boolean;
}

export const Container = styled.div<ContainerProps>(({ size, width, $minWidth, $selectLabelVariant, isSelected }) => {
    const getMinWidth = () => {
        if ($minWidth) return $minWidth;
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

export const DropdownContainer = styled.div<{ ignoreMaxHeight?: boolean }>(({ ignoreMaxHeight, theme }) => ({
    ...getDropdownStyle(),
    borderRadius: radius.lg,
    background: theme?.colors?.bg,
    zIndex: zIndices.dropdown,
    transition: sharedTransition,
    boxShadow: theme?.colors?.shadowMd,
    padding: spacing.xsm,
    display: 'flex',
    flexDirection: 'column',
    gap: '8px',
    marginTop: '4px',
    overflow: 'auto',
    width: '100%',
    maxHeight: ignoreMaxHeight ? undefined : '360px',
    // Force a persistent scrollbar so overflowing options (e.g. the language list)
    // are discoverable; WebKit overlay scrollbars otherwise auto-hide on macOS.
    '&::-webkit-scrollbar': {
        width: '6px',
        background: theme?.colors?.scrollbarTrack,
    },
    '&::-webkit-scrollbar-thumb': {
        background: theme?.colors?.scrollbarThumb,
        borderRadius: radius.lg,
    },
}));

// Styled components for SelectValue (Selected value display)
export const SelectValue = styled.span(({ theme }) => ({
    ...inputValueTextStyles(),
    color: theme?.colors?.text,
    minWidth: 0,
    // Match SelectLabelContainer — line-height 1 + overflow hidden clips glyph descenders.
    lineHeight: typography.lineHeights.sm,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
}));

export const Placeholder = styled.span(({ theme }) => ({
    ...inputPlaceholderTextStyles,
    color: theme?.colors?.textPlaceholder,
}));

export const ActionButtonsContainer = styled.div({
    display: 'flex',
    gap: '6px',
    flexDirection: 'row',
    alignItems: 'center',
    minWidth: 0,
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
    width: '100%',
});

export const DescriptionContainer = styled.span(({ theme }) => ({
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    width: '100%',
    color: theme?.colors?.textSecondary,
    lineHeight: 'normal',
    fontSize: typography.fontSizes.sm,
    marginTop: spacing.xxsm,
}));

export const LabelsWrapper = styled.div<{ shouldShowGap?: boolean }>(({ shouldShowGap = false }) => ({
    display: 'flex',
    flexWrap: 'wrap',
    gap: shouldShowGap ? spacing.xxsm : '0px',
    maxHeight: '150px',
    maxWidth: '100%',
    minWidth: 0,
}));

export const OptionLabel = styled.label<{
    isSelected: boolean;
    isMultiSelect?: boolean;
    isDisabled?: boolean;
    applyHoverWidth?: boolean;
}>(({ isSelected, isMultiSelect, isDisabled, applyHoverWidth, theme }) => ({
    ...getOptionLabelStyle(isSelected, isMultiSelect, isDisabled, applyHoverWidth, theme),
}));

export const SelectLabel = styled.label(({ theme }) => ({
    ...formLabelTextStyles,
    color: theme.colors.text,
    marginBottom: spacing.xxsm,
    textAlign: 'left',
}));

export const StyledIcon = styled(Icon)(({ theme }) => ({
    flexShrink: 0,
    color: theme?.colors?.text,
}));

export { Checkbox as StyledCheckbox } from '@components/components/Checkbox';

export const Required = styled.span(({ theme }) => ({
    color: theme.colors.textError,
}));
