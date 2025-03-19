import React from 'react';
import { LabelContainer, SelectAllOption, StyledCheckbox } from '../../components';

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
