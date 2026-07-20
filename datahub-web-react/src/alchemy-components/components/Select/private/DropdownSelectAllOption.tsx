import React from 'react';
import styled from 'styled-components';

import { LabelContainer, StyledCheckbox } from '@components/components/Select/components';
import { spacing, typography } from '@components/theme';

const SelectAllOption = styled.button<{ isDisabled?: boolean }>(({ isDisabled, theme }) => ({
    cursor: isDisabled ? 'not-allowed' : 'pointer',
    padding: spacing.xsm,
    color: theme?.colors?.text,
    fontWeight: typography.fontWeights.semiBold,
    fontSize: typography.fontSizes.md,
    display: 'flex',
    alignItems: 'center',
    background: 'transparent',
    border: 'none',
    width: '100%',
    textAlign: 'left',
}));

interface Props {
    label?: string;
    selected: boolean;
    disabled?: boolean;
    onClick?: () => void;
}

export default function DropdownSelectAllOption({ label, selected, onClick, disabled }: Props) {
    return (
        <SelectAllOption
            type="button"
            onClick={() => !disabled && onClick?.()}
            isDisabled={disabled}
            aria-pressed={selected}
        >
            <LabelContainer>
                <span>{label}</span>
                <StyledCheckbox
                    isChecked={selected}
                    isDisabled={disabled}
                    onCheckboxChange={() => onClick?.()}
                    size="sm"
                />
            </LabelContainer>
        </SelectAllOption>
    );
}
