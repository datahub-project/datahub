/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
