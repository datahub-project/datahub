// CheckboxGroup.tsx
import React, { ReactNode } from 'react';
import styled from 'styled-components';
import { Checkbox } from '../Checkbox';

const StyledCheckboxLabel = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: 97%;
    padding: 0px 6px 0px 4px;
    cursor: pointer;
`;

const StyledCheckboxGroup = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    margin-bottom: 12px;
`;

interface SelectItemCheckboxGroupProps {
    options: Array<{ value: string; label: ReactNode | string }>;
    handleCheckboxToggle: (value: string) => void;
    renderOption?: (option: { value: string; label: ReactNode | string }) => React.ReactNode;
    selectedOptions: string[];
}

export const SelectItemCheckboxGroup: React.FC<SelectItemCheckboxGroupProps> = ({
    options,
    handleCheckboxToggle,
    renderOption,
    selectedOptions,
}) => {
    return (
        <StyledCheckboxGroup>
            {options.map((option) => (
                <StyledCheckboxLabel key={option.value} onClick={() => handleCheckboxToggle(option.value)}>
                    {renderOption ? renderOption(option) : <span>{option.label}</span>}
                    <Checkbox
                        value={option.value}
                        isChecked={selectedOptions.includes(option.value)}
                        onChange={() => handleCheckboxToggle(option.value)}
                    />
                </StyledCheckboxLabel>
            ))}
        </StyledCheckboxGroup>
    );
};
