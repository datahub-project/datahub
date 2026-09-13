import { Pill } from '@components';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React, { useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';

import {
    ClearButton,
    ErrorMessage,
    HelperText,
    InputContainer,
    InputWrapper,
    Label,
    NativeInput,
    TagsWrapper,
} from '@components/components/MultiSelectInput/components';
import { MultiSelectInputProps } from '@components/components/MultiSelectInput/types';

export const MultiSelectInput = ({
    values,
    onUpdate,
    placeholder,
    label,
    error,
    helperText,
    disabled = false,
    inputTestId,
    id,
    className,
    width = 300,
    inputType = 'text',
}: MultiSelectInputProps) => {
    const { t } = useTranslation('alchemy');
    const [inputValue, setInputValue] = useState('');
    const inputRef = useRef<HTMLInputElement>(null);

    const handleInputChange = (newValue: string) => {
        setInputValue(newValue);
    };

    // Helper: Commit pending input value if valid
    const commitPendingValue = () => {
        const trimmedValue = inputValue.trim();
        if (trimmedValue && !values.includes(trimmedValue)) {
            onUpdate([...values, trimmedValue]);
        }
    };

    const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
        if (e.key === 'Enter' || e.key === ',') {
            e.preventDefault();
            e.stopPropagation();
            commitPendingValue();
            setInputValue('');
        } else if (e.key === 'Backspace' && inputValue === '' && values.length > 0) {
            onUpdate(values.slice(0, -1));
        }
    };

    const handleRemoveTag = (tagToRemove: string, e: React.MouseEvent) => {
        // Prevent blur from firing on the input
        e.preventDefault();
        const newValues = values.filter((v) => v !== tagToRemove);
        onUpdate(newValues);
        // Refocus input to prevent blur from committing pending text
        inputRef.current?.focus();
    };

    const handleClearAll = (e: React.MouseEvent) => {
        // Prevent blur from firing on the input
        e.preventDefault();
        onUpdate([]);
        setInputValue('');
    };

    // Commit pending input value when input loses focus, always clear input
    const handleBlur = () => {
        commitPendingValue();
        setInputValue('');
    };

    const showError = !!error;

    return (
        <InputWrapper id={id} className={className}>
            {label && <Label>{label}</Label>}
            <InputContainer isInvalid={showError ? true : undefined} $width={width}>
                <TagsWrapper>
                    {values.map((tag) => (
                        <Pill
                            key={tag}
                            label={tag}
                            rightIcons={[
                                {
                                    icon: X,
                                    onClick: (e) => handleRemoveTag(tag, e),
                                    ariaLabel: t('multiSelectInput.removeTagAriaLabel', { tag }),
                                    testId: `remove-tag-${tag}`,
                                },
                            ]}
                            dataTestId={`pill-${tag}`}
                        />
                    ))}
                    <NativeInput
                        ref={inputRef}
                        type={inputType}
                        value={inputValue}
                        onChange={(e) => handleInputChange(e.target.value)}
                        placeholder={values.length === 0 ? placeholder : undefined}
                        onKeyDown={handleKeyDown}
                        onBlur={handleBlur}
                        disabled={disabled}
                        data-testid={inputTestId}
                    />
                </TagsWrapper>
                {values.length > 0 && (
                    <ClearButton
                        onClick={(e) => handleClearAll(e)}
                        disabled={disabled}
                        data-testid="clear-all-button"
                        icon={{ icon: X, size: 'lg', color: 'icon' }}
                        variant="text"
                        isCircle
                        aria-label={t('multiSelectInput.clearAllAriaLabel')}
                    />
                )}
            </InputContainer>
            {showError && error && <ErrorMessage>{error}</ErrorMessage>}
            {!showError && helperText && <HelperText>{helperText}</HelperText>}
        </InputWrapper>
    );
};
