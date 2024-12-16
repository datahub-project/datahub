import React from 'react';

import { TextAreaProps } from './types';

import {
    ErrorMessage,
    Label,
    Required,
    StyledIcon,
    StyledStatusIcon,
    TextAreaContainer,
    TextAreaField,
    TextAreaWrapper,
    WarningMessage,
} from './components';

export const textAreaDefaults: TextAreaProps = {
    label: 'Label',
    placeholder: 'Placeholder',
    error: '',
    warning: '',
    isSuccess: false,
    isDisabled: false,
    isInvalid: false,
    isReadOnly: false,
    isRequired: false,
};

export const TextArea = ({
    label = textAreaDefaults.label,
    placeholder = textAreaDefaults.placeholder,
    icon, // default undefined
    error = textAreaDefaults.error,
    warning = textAreaDefaults.warning,
    isSuccess = textAreaDefaults.isSuccess,
    isDisabled = textAreaDefaults.isDisabled,
    isInvalid = textAreaDefaults.isInvalid,
    isReadOnly = textAreaDefaults.isReadOnly,
    isRequired = textAreaDefaults.isRequired,
    ...props
}: TextAreaProps) => {
    // Invalid state is always true if error is present
    let invalid = isInvalid;
    if (error) invalid = true;

    // Input base props

    const textAreaBaseProps = {
        label,
        isSuccess,
        error,
        warning,
        isDisabled,
        isInvalid: invalid,
    };

    return (
        <TextAreaWrapper>
            <Label>
                {label} {isRequired && <Required>*</Required>}
            </Label>
            <TextAreaContainer {...textAreaBaseProps}>
                {icon && <StyledIcon icon={icon} size="lg" />}
                <TextAreaField
                    icon={icon}
                    placeholder={placeholder}
                    readOnly={isReadOnly}
                    disabled={isDisabled}
                    required={isRequired}
                    {...props}
                />
                {isSuccess && <StyledStatusIcon icon="CheckCircle" color="green" size="lg" />}
                {invalid && <StyledStatusIcon icon="WarningAmber" color="red" size="lg" />}
                {warning && <StyledStatusIcon icon="ErrorOutline" color="yellow" size="lg" />}
            </TextAreaContainer>
            {invalid && error && <ErrorMessage>{error}</ErrorMessage>}
            {warning && <WarningMessage>{warning}</WarningMessage>}
        </TextAreaWrapper>
    );
};
