import { IconProps } from '@components/components/Icon/types';
import React, { InputHTMLAttributes } from 'react';

export interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
    value?: string | number | readonly string[] | undefined;
    setValue?: React.Dispatch<React.SetStateAction<string>>;
    label: string;
    placeholder?: string;
    icon?: IconProps;
    error?: string;
    warning?: string;
    isSuccess?: boolean;
    isDisabled?: boolean;
    isInvalid?: boolean;
    isReadOnly?: boolean;
    isPassword?: boolean;
    isRequired?: boolean;
    errorOnHover?: boolean;
    id?: string;
    type?: string;
}
