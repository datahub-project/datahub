import { InputHTMLAttributes } from 'react';

import { SizeOptions } from '@src/alchemy-components/theme/config';

export interface CheckboxProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'size'> {
    label?: string;
    labelTooltip?: string;
    error?: string;
    isChecked?: boolean;
    setIsChecked?: (isChecked: boolean) => void;
    isDisabled?: boolean;
    isIntermediate?: boolean;
    isRequired?: boolean;
    onCheckboxChange?: (isChecked: boolean) => void;
    size?: SizeOptions;
    dataTestId?: string;
    justifyContent?: 'center' | 'flex-start';
    gap?: string;
    shouldHandleLabelClicks?: boolean;
}

export interface CheckboxGroupProps {
    isVertical?: boolean;
    checkboxes: CheckboxProps[];
}
