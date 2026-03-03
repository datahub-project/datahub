import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { Icon } from '@components/components/Icon';
import {
    ErrorMessage,
    HelperText,
    InputContainer,
    InputField,
    InputWrapper,
    Label,
    Required,
    WarningMessage,
} from '@components/components/Input/components';
import { InputProps } from '@components/components/Input/types';
import { getInputType } from '@components/components/Input/utils';

export const inputDefaults: InputProps = {
    value: '',
    setValue: () => {},
    label: '',
    placeholder: 'Placeholder',
    error: '',
    warning: '',
    helperText: '',
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

const ClearIcon = styled(Icon)`
    cursor: pointer;
`;

export const Input = ({
    value = inputDefaults.value,
    setValue = inputDefaults.setValue,
    label = inputDefaults.label,
    placeholder = inputDefaults.placeholder,
    icon, // default undefined
    error = inputDefaults.error,
    warning = inputDefaults.warning,
    helperText = inputDefaults.helperText,
    isSuccess = inputDefaults.isSuccess,
    isDisabled = inputDefaults.isDisabled,
    isInvalid = inputDefaults.isInvalid,
    isReadOnly = inputDefaults.isReadOnly,
    isPassword = inputDefaults.isPassword,
    isRequired = inputDefaults.isRequired,
    errorOnHover = inputDefaults.errorOnHover,
    type = inputDefaults.type,
    id,
    inputStyles,
    inputTestId,
    onClear,
    maxLength,
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
                {icon && <SearchIcon size="xl" {...icon} />}
                <InputField
                    value={value}
                    onChange={(e) => setValue?.(e.target.value)}
                    type={getInputType(type, isPassword, showPassword)}
                    placeholder={placeholder}
                    readOnly={isReadOnly}
                    disabled={isDisabled}
                    required={isRequired}
                    id={id}
                    maxLength={maxLength}
                    style={{ paddingLeft: icon ? '8px' : '', ...inputStyles }}
                    data-testid={inputTestId}
                />
                {!isPassword && (
                    <Tooltip title={errorOnHover ? error : ''} showArrow={false}>
                        {invalid && <Icon icon="WarningAmber" color="red" size="lg" />}
                        {isSuccess && <Icon icon="CheckCircle" color="green" size="lg" />}
                        {warning && <Icon icon="ErrorOutline" color="yellow" size="lg" />}
                    </Tooltip>
                )}
                {!!onClear && value && (
                    <ClearIcon className="clear-search" source="phosphor" icon="X" size="lg" onClick={onClear} />
                )}
                {isPassword && <Icon onClick={() => setShowPassword(!showPassword)} icon={passwordIcon} size="lg" />}
            </InputContainer>
            {invalid && error && !errorOnHover && <ErrorMessage>{error}</ErrorMessage>}
            {warning && <WarningMessage>{warning}</WarningMessage>}
            {helperText && !invalid && !warning && <HelperText>{helperText}</HelperText>}
        </InputWrapper>
    );
};
