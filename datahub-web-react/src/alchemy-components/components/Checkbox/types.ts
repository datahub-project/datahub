/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { InputHTMLAttributes } from 'react';

import { SizeOptions } from '@src/alchemy-components/theme/config';

export interface CheckboxProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'size'> {
    label?: string;
    labelTooltip?: string;
    error?: string;
    isChecked?: boolean;
    setIsChecked?: React.Dispatch<React.SetStateAction<boolean>>;
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
