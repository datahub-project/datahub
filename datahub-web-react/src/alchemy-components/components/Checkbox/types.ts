import { InputHTMLAttributes } from 'react';

export interface CheckboxProps extends InputHTMLAttributes<HTMLInputElement> {
    label: string;
    error?: string;
    isChecked?: boolean;
    setIsChecked?: React.Dispatch<React.SetStateAction<boolean>>;
    isDisabled?: boolean;
    isIntermediate?: boolean;
    isRequired?: boolean;
}

export interface CheckboxGroupProps {
    isVertical?: boolean;
    checkboxes: CheckboxProps[];
}
