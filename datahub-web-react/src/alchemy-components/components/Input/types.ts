import { InputHTMLAttributes } from 'react';

import { IconNames } from '../Icon';

export interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
    value?: string | number | readonly string[] | undefined;
    setValue?: React.Dispatch<React.SetStateAction<string>>;
    label: string;
    placeholder?: string;
    icon?: IconNames;
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
