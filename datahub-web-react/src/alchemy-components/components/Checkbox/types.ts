import { InputHTMLAttributes } from 'react';
import { SizeOptions } from '@src/alchemy-components/theme/config';

export interface CheckboxProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'size'> {
    label?: string;
    error?: string;
    isChecked?: boolean;
    setIsChecked?: React.Dispatch<React.SetStateAction<boolean>>;
    isDisabled?: boolean;
    isIntermediate?: boolean;
    isRequired?: boolean;
    onCheckboxChange?: () => void;
    size?: SizeOptions;
}

export interface CheckboxGroupProps {
    isVertical?: boolean;
    checkboxes: CheckboxProps[];
}
