import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { Warning } from '@phosphor-icons/react/dist/csr/Warning';
import React from 'react';
import { useTranslation } from 'react-i18next';

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
} from '@components/components/TextArea/components';
import { TextAreaProps } from '@components/components/TextArea/types';

export const textAreaDefaults: TextAreaProps = {
    label: '',
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
    placeholder: placeholderProp,
    icon, // default undefined
    error = textAreaDefaults.error,
    warning = textAreaDefaults.warning,
    isSuccess = textAreaDefaults.isSuccess,
    isDisabled = textAreaDefaults.isDisabled,
    isInvalid = textAreaDefaults.isInvalid,
    isReadOnly = textAreaDefaults.isReadOnly,
    isRequired = textAreaDefaults.isRequired,
    inputTestId,
    ...props
}: TextAreaProps) => {
    const { t } = useTranslation('alchemy');
    const placeholder = placeholderProp ?? t('textArea.placeholder');
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
            {label && (
                <Label aria-label={label}>
                    {label} {isRequired && <Required>*</Required>}
                </Label>
            )}
            <TextAreaContainer {...textAreaBaseProps}>
                {icon && <StyledIcon icon={icon} size="lg" />}
                <TextAreaField
                    icon={icon}
                    placeholder={placeholder}
                    readOnly={isReadOnly}
                    disabled={isDisabled}
                    required={isRequired}
                    data-testid={inputTestId}
                    {...props}
                />
                {isSuccess && <StyledStatusIcon icon={Check} color="green" size="lg" />}
                {invalid && <StyledStatusIcon icon={Warning} color="red" size="lg" />}
                {warning && <StyledStatusIcon icon={Warning} color="yellow" size="lg" />}
            </TextAreaContainer>
            {invalid && error && <ErrorMessage>{error}</ErrorMessage>}
            {warning && <WarningMessage>{warning}</WarningMessage>}
        </TextAreaWrapper>
    );
};
