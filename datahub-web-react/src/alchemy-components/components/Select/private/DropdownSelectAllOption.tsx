import React, { useCallback } from 'react';
import styled from 'styled-components';

import { LabelContainer, StyledCheckbox } from '@components/components/Select/components';
import { spacing, typography } from '@components/theme';

const SelectAllOption = styled.div<{ isDisabled?: boolean }>(({ isDisabled, theme }) => ({
    cursor: isDisabled ? 'not-allowed' : 'pointer',
    padding: spacing.xsm,
    color: theme?.colors?.text,
    fontWeight: typography.fontWeights.semiBold,
    fontSize: typography.fontSizes.md,
    display: 'flex',
    alignItems: 'center',
    '&:focus-visible': {
        outline: `2px solid ${theme.colors.borderBrandFocused}`,
        outlineOffset: '2px',
    },
}));

interface Props {
    label?: string;
    selected: boolean;
    disabled?: boolean;
    onClick?: () => void;
}

export default function DropdownSelectAllOption({ label, selected, onClick, disabled }: Props) {
    const handleActivate = useCallback(() => {
        if (!disabled) onClick?.();
    }, [disabled, onClick]);

    const handleKeyDown = useCallback(
        (event: React.KeyboardEvent<HTMLElement>) => {
            if (event.key !== 'Enter' && event.key !== ' ') return;
            event.preventDefault();
            handleActivate();
        },
        [handleActivate],
    );

    return (
        <SelectAllOption
            role="option"
            aria-selected={selected}
            aria-disabled={disabled}
            tabIndex={-1}
            onClick={handleActivate}
            onKeyDown={handleKeyDown}
            isDisabled={disabled}
        >
            <LabelContainer>
                <span>{label}</span>
                <span aria-hidden="true">
                    <StyledCheckbox
                        tabIndex={-1}
                        isChecked={selected}
                        isDisabled={disabled}
                        onCheckboxChange={handleActivate}
                        size="sm"
                    />
                </span>
            </LabelContainer>
        </SelectAllOption>
    );
}
