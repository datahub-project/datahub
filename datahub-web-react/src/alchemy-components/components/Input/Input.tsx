import { Button, Tooltip } from '@components';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { Eye } from '@phosphor-icons/react/dist/csr/Eye';
import { EyeSlash } from '@phosphor-icons/react/dist/csr/EyeSlash';
import { Warning } from '@phosphor-icons/react/dist/csr/Warning';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React from 'react';
import { useTranslation } from 'react-i18next';
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

const ActionButton = styled(Button)`
    padding: 0;
    min-width: unset;
    flex-shrink: 0;
`;

export const Input = ({
    value = inputDefaults.value,
    setValue = inputDefaults.setValue,
    label = inputDefaults.label,
    placeholder,
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
    ariaLabel,
    inputStyles,
    inputTestId,
    onClear,
    maxLength,
    ...props
}: InputProps) => {
    const { t } = useTranslation('alchemy');
    const resolvedPlaceholder = placeholder ?? t('input.placeholder');
    const clearLabel = t('input.clear');
    const showPasswordLabel = t('input.showPassword');
    const hidePasswordLabel = t('input.hidePassword');

    // Invalid state is always true if error is present
    let invalid = isInvalid;
    if (error) invalid = true;

    // Show/hide password text
    const [showPassword, setShowPassword] = React.useState(false);
    const passwordIcon = showPassword ? Eye : EyeSlash;

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
                <Label htmlFor={id}>
                    {label} {isRequired && <Required>*</Required>}
                </Label>
            )}
            <InputContainer {...inputBaseProps}>
                {icon && <SearchIcon size="xl" {...icon} />}
                <InputField
                    value={value}
                    onChange={(e) => setValue?.(e.target.value)}
                    type={getInputType(type, isPassword, showPassword)}
                    placeholder={resolvedPlaceholder}
                    readOnly={isReadOnly}
                    disabled={isDisabled}
                    required={isRequired}
                    id={id}
                    aria-label={ariaLabel}
                    maxLength={maxLength}
                    style={{ paddingLeft: icon ? '8px' : '', ...inputStyles }}
                    data-testid={inputTestId}
                />
                {!isPassword && (
                    <Tooltip title={errorOnHover ? error : ''} showArrow={false}>
                        {invalid && <Icon icon={Warning} color="red" size="lg" />}
                        {isSuccess && <Icon icon={Check} color="green" size="lg" />}
                        {warning && <Icon icon={Warning} color="yellow" size="lg" />}
                    </Tooltip>
                )}
                {!!onClear && value && (
                    <ActionButton type="button" variant="text" color="gray" aria-label={clearLabel} onClick={onClear}>
                        <Icon icon={X} size="lg" />
                    </ActionButton>
                )}
                {isPassword && (
                    <ActionButton
                        type="button"
                        variant="text"
                        color="gray"
                        aria-label={showPassword ? hidePasswordLabel : showPasswordLabel}
                        aria-pressed={showPassword}
                        onClick={() => setShowPassword(!showPassword)}
                    >
                        <Icon icon={passwordIcon} size="lg" />
                    </ActionButton>
                )}
            </InputContainer>
            {invalid && error && !errorOnHover && <ErrorMessage>{error}</ErrorMessage>}
            {warning && <WarningMessage>{warning}</WarningMessage>}
            {helperText && !invalid && !warning && <HelperText>{helperText}</HelperText>}
        </InputWrapper>
    );
};
