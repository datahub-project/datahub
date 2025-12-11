/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { InputHTMLAttributes } from 'react';

import { IconProps } from '@components/components/Icon/types';

export interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
    value?: string | number | readonly string[] | undefined;
    setValue?: React.Dispatch<React.SetStateAction<string>>;
    label: string;
    placeholder?: string;
    icon?: IconProps;
    error?: string;
    warning?: string;
    helperText?: string;
    isSuccess?: boolean;
    isDisabled?: boolean;
    isInvalid?: boolean;
    isReadOnly?: boolean;
    isPassword?: boolean;
    isRequired?: boolean;
    errorOnHover?: boolean;
    id?: string;
    type?: string;
    styles?: React.CSSProperties;
    inputStyles?: React.CSSProperties;
    inputTestId?: string;
    onClear?: () => void;
}
