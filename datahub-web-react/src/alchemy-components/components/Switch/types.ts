import { InputHTMLAttributes } from 'react';
import { CSSProperties } from 'styled-components';

import { IconNames } from '@components/components/Icon';
import { ColorOptions, SizeOptions } from '@components/theme/config';

export type SwitchLabelPosition = 'left' | 'top';

export interface SwitchProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'size'> {
    label: string;
    labelPosition?: SwitchLabelPosition;
    icon?: IconNames;
    colorScheme?: ColorOptions;
    size?: SizeOptions;
    isSquare?: boolean;
    isChecked?: boolean;
    isDisabled?: boolean;
    isRequired?: boolean;
    labelHoverText?: string;
    disabledHoverText?: string;
    labelStyle?: CSSProperties;
}
