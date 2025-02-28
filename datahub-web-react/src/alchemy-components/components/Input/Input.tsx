import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { InputProps } from './types';

import { ErrorMessage, InputContainer, InputField, InputWrapper, Label, Required, WarningMessage } from './components';

import { Icon } from '../Icon';
import { getInputType } from './utils';

export const inputDefaults: InputProps = {
    value: '',
    setValue: () => {},
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
    errorOnHover: false,
    type: 'text',
};

const SearchIcon = styled(Icon)`
    margin-left: 8px;
`;

export const Input = ({
    value = inputDefaults.value,
    setValue = inputDefaults.setValue,
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
    errorOnHover = inputDefaults.errorOnHover,
    type = inputDefaults.type,
    id,
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
        type,
        label,
        isSuccess,
        error,
        warning,
        isDisabled,
        isInvalid: invalid,
    };

    return (
        <InputWrapper {...props}>
            {label && (
                <Label aria-label={label}>
                    {label} {isRequired && <Required>*</Required>}
                </Label>
            )}
            <InputContainer {...inputBaseProps}>
                {icon && <SearchIcon icon={icon.name} source={icon.source} variant={icon.variant} size="xl" />}
                <InputField
                    value={value}
                    onChange={(e) => setValue?.(e.target.value)}
                    type={getInputType(type, isPassword, showPassword)}
                    placeholder={placeholder}
                    readOnly={isReadOnly}
                    disabled={isDisabled}
                    required={isRequired}
                    id={id}
                    style={{ paddingLeft: icon ? '8px' : '' }}
                />
                {!isPassword && (
                    <Tooltip title={errorOnHover ? error : ''} showArrow={false}>
                        {invalid && <Icon icon="WarningAmber" color="red" size="lg" />}
                        {isSuccess && <Icon icon="CheckCircle" color="green" size="lg" />}
                        {warning && <Icon icon="ErrorOutline" color="yellow" size="lg" />}
                    </Tooltip>
                )}
                {isPassword && <Icon onClick={() => setShowPassword(!showPassword)} icon={passwordIcon} size="lg" />}
            </InputContainer>
            {invalid && error && !errorOnHover && <ErrorMessage>{error}</ErrorMessage>}
            {warning && <WarningMessage>{warning}</WarningMessage>}
        </InputWrapper>
    );
};
