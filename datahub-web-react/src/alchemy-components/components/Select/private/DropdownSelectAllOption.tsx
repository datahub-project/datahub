import React from 'react';
import styled from 'styled-components';
<<<<<<< HEAD

import { LabelContainer, StyledCheckbox } from '@components/components/Select/components';
import { colors, spacing, typography } from '@components/theme';
=======
import { colors, spacing, typography } from '@components/theme';
import { LabelContainer, StyledCheckbox } from '../components';
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105

const SelectAllOption = styled.div<{ isSelected: boolean; isDisabled?: boolean }>(({ isSelected, isDisabled }) => ({
    cursor: isDisabled ? 'not-allowed' : 'pointer',
    padding: spacing.xsm,
    color: isSelected ? colors.violet[700] : colors.gray[500],
    fontWeight: typography.fontWeights.semiBold,
    fontSize: typography.fontSizes.md,
    display: 'flex',
    alignItems: 'center',
}));

interface Props {
    label?: string;
    selected: boolean;
    disabled?: boolean;
    onClick?: () => void;
}

export default function DropdownSelectAllOption({ label, selected, onClick, disabled }: Props) {
    return (
        <SelectAllOption isSelected={selected} onClick={onClick} isDisabled={disabled}>
            <LabelContainer>
                <span>{label}</span>
                <StyledCheckbox checked={selected} disabled={disabled} />
            </LabelContainer>
        </SelectAllOption>
    );
}
