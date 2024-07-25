import { SizeOptions, ColorOptions } from '@components/theme/config';
import { InputHTMLAttributes } from 'react';
import { IconNames } from '../Icon';

export interface SwitchProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'size'> {
    label: string;
    icon?: IconNames;
    colorScheme?: ColorOptions;
    size?: SizeOptions;
    isSquare?: boolean;
    isChecked?: boolean;
    isDisabled?: boolean;
    isIntermediate?: boolean;
    isRequired?: boolean;
}
