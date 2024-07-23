import React from 'react';

import { InputProps } from './types';

import { InputWrapper, InputContainer, InputField, Label, Required, WarningMessage, ErrorMessage } from './components';

import { Icon } from '../Icon';

export const inputDefaults: InputProps = {
    label: 'Label',
    placeholder: 'Placeholder',
    error: '',
    warning: '',
    isSuccess: false,
    isDisabled: false,
    isInvalid: false,
    isReadOnly: false,
    isPassword: false,
    isRequired: false,
};

export const Input = ({
    label = inputDefaults.label,
    placeholder = inputDefaults.placeholder,
    icon, // default undefined
    error = inputDefaults.error,
    warning = inputDefaults.warning,
    isSuccess = inputDefaults.isSuccess,
    isDisabled = inputDefaults.isDisabled,
    isInvalid = inputDefaults.isInvalid,
    isReadOnly = inputDefaults.isReadOnly,
    isPassword = inputDefaults.isPassword,
    isRequired = inputDefaults.isRequired,
    ...props
}: InputProps) => {
    // Invalid state is always true if error is present
    let invalid = isInvalid;
    if (error) invalid = true;

    // Show/hide password text
    const [showPassword, setShowPassword] = React.useState(false);
    const passwordIcon = showPassword ? 'Visibility' : 'VisibilityOff';

    // Input base props
    const inputBaseProps = {
        label,
        isSuccess,
        error,
        warning,
        isDisabled,
        isInvalid: invalid,
    };

    return (
        <InputWrapper {...props}>
            <Label>
                {label} {isRequired && <Required>*</Required>}
            </Label>
            <InputContainer {...inputBaseProps}>
                {icon && <Icon icon={icon} size="lg" />}
                <InputField
                    type={isPassword && !showPassword ? 'password' : 'text'}
                    placeholder={placeholder}
                    readOnly={isReadOnly}
                    disabled={isDisabled}
                    required={isRequired}
                />
                {!isPassword && (
                    <>
                        {invalid && <Icon icon="WarningAmber" color="red" size="lg" />}
                        {isSuccess && <Icon icon="CheckCircle" color="green" size="lg" />}
                        {warning && <Icon icon="ErrorOutline" color="yellow" size="lg" />}
                    </>
                )}
                {isPassword && <Icon onClick={() => setShowPassword(!showPassword)} icon={passwordIcon} size="lg" />}
            </InputContainer>
            {invalid && error && <ErrorMessage>{error}</ErrorMessage>}
            {warning && <WarningMessage>{warning}</WarningMessage>}
        </InputWrapper>
    );
};
