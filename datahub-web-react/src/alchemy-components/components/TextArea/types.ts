import { TextareaHTMLAttributes } from 'react';
import { IconNames } from '../Icon';

export interface TextAreaProps extends TextareaHTMLAttributes<HTMLTextAreaElement> {
    label: string;
    placeholder?: string;
    icon?: IconNames;
    error?: string;
    warning?: string;
    isSuccess?: boolean;
    isDisabled?: boolean;
    isInvalid?: boolean;
    isReadOnly?: boolean;
    isRequired?: boolean;
}
