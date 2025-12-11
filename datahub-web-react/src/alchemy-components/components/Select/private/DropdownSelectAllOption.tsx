/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { LabelContainer, StyledCheckbox } from '@components/components/Select/components';
import { colors, spacing, typography } from '@components/theme';

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
