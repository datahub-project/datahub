import { InputHTMLAttributes } from 'react';

export interface RadioProps extends InputHTMLAttributes<HTMLInputElement> {
    label?: string;
    error?: string;
    isChecked?: boolean;
    setIsChecked?: React.Dispatch<React.SetStateAction<boolean>>;
    isDisabled?: boolean;
    isIntermediate?: boolean;
    isRequired?: boolean;
}

export interface RadioGroupProps {
    isVertical?: boolean;
    radios: RadioProps[];
}
