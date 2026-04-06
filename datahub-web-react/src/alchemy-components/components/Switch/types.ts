import React, { InputHTMLAttributes } from 'react';
import { CSSProperties } from 'styled-components';

import { ColorOptions, SizeOptions } from '@components/theme/config';

export type SwitchLabelPosition = 'left' | 'top' | 'right';

export interface SwitchProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'size'> {
    label: string;
    labelPosition?: SwitchLabelPosition;
    icon?: React.ComponentType<any>;
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
