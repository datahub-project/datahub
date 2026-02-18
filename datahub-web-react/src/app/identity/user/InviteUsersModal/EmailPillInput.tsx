import { Icon, Pill, colors } from '@components';
import React, { useCallback, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import { colors as alchemyColors, typography } from '@src/alchemy-components/theme';

type ParsedEmail = {
    id: string;
    email: string;
    isValid: boolean;
};

type Props = {
    emails: string[];
    onEmailsChange: (emails: string[]) => void;
    onKeyPress: (e: React.KeyboardEvent<HTMLInputElement>) => void;
    onInputChange?: (value: string) => void;
    className?: string;
    placeholder?: string;
    helperText?: string;
    error?: string;
};

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

const Container = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    flex: 1;
    align-items: flex-start;
`;

const InputWrapper = styled.div`
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 6px;
    border: 1px solid ${alchemyColors.gray[100]};
    border-radius: 8px;
    padding: 8px 12px;
    min-height: 36px;
    background: white;
    cursor: text;
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
    transition: all 0.1s ease;

    &:focus-within {
        border-color: ${alchemyColors.violet[200]};
        outline: 1px solid ${alchemyColors.violet[200]};
    }
`;

const PillsContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
    align-items: center;
    flex: 1;
    width: 400px;
`;

const HiddenInput = styled.input`
    border: none;
    outline: none;
    background: transparent;
    flex: 1;
    min-width: 200px;
    color: ${alchemyColors.gray[600]};
    font-size: ${typography.fontSizes.md};
    padding: 0;
    margin: 0;

    &::placeholder {
        color: ${alchemyColors.gray[400]};
    }
`;

const HelperText = styled.div`
    font-size: 12px;
    color: #4b5563;
    margin-top: 6px;
    line-height: 16px;
`;

const ErrorText = styled.div`
    font-size: 12px;
    color: ${colors.red[1000]};
    margin-top: 6px;
    line-height: 16px;
`;

const IconWrapper = styled.div`
    display: flex;
    align-items: center;
    color: #6b7280;
    padding-top: 2px; /* Align with first line of content */
    flex-shrink: 0;
`;

export default function EmailPillInput({
    emails,
    onEmailsChange,
    onKeyPress,
    onInputChange,
    className,
    placeholder = 'email1@address.com, email2@address.com',
    helperText = 'Input emails separated by commas, then press Enter to finish.',
    error,
}: Props) {
    const [inputValue, setInputValue] = useState('');
    const inputRef = useRef<HTMLInputElement>(null);

    const nextId = useRef(0);
    const parsedEmails: ParsedEmail[] = useMemo(
        () => emails.map((email) => ({ id: `email-${nextId.current++}`, email, isValid: EMAIL_REGEX.test(email) })),
        [emails],
    );

    const validationError = useMemo(() => {
        const invalidEmails = parsedEmails.filter((e) => !e.isValid);
        if (invalidEmails.length > 0) {
            return 'Enter valid email address';
        }
        return '';
    }, [parsedEmails]);

    const setInput = useCallback(
        (value: string) => {
            setInputValue(value);
            onInputChange?.(value);
        },
        [onInputChange],
    );

    const parseRawInput = useCallback((input: string): string[] => {
        return input
            .split(/[,\s]+/)
            .map((email) => email.trim())
            .filter((email) => email.length > 0);
    }, []);

    const commitInput = useCallback(
        (raw: string) => {
            const newEmails = parseRawInput(raw);
            if (newEmails.length > 0) {
                onEmailsChange([...emails, ...newEmails]);
                setInput('');
            }
        },
        [emails, parseRawInput, onEmailsChange, setInput],
    );

    const handleInputChange = useCallback(
        (e: React.ChangeEvent<HTMLInputElement>) => {
            const { value } = e.target;

            if (value.includes(',') || value.includes(' ')) {
                commitInput(value);
                return;
            }

            setInput(value);
        },
        [commitInput, setInput],
    );

    const handleKeyDown = useCallback(
        (e: React.KeyboardEvent<HTMLInputElement>) => {
            if (e.key === 'Enter' || e.key === ',') {
                e.preventDefault();
                if (inputValue.trim()) {
                    commitInput(inputValue);
                }
            } else if (e.key === 'Backspace' && !inputValue && emails.length > 0) {
                onEmailsChange(emails.slice(0, -1));
            }

            onKeyPress(e);
        },
        [inputValue, emails, commitInput, onEmailsChange, onKeyPress],
    );

    const handlePillRemove = useCallback(
        (indexToRemove: number) => {
            onEmailsChange(emails.filter((_, index) => index !== indexToRemove));
        },
        [emails, onEmailsChange],
    );

    const handleBlur = useCallback(() => {
        if (inputValue.trim()) {
            commitInput(inputValue);
        }
    }, [inputValue, commitInput]);

    const handleContainerClick = useCallback(() => {
        inputRef.current?.focus();
    }, []);

    const currentError = error || validationError;

    return (
        <Container className={className}>
            <InputWrapper onClick={handleContainerClick}>
                <IconWrapper>
                    <Icon icon="EnvelopeSimple" source="phosphor" size="xl" />
                </IconWrapper>
                <PillsContainer>
                    {parsedEmails.map((parsedEmail, index) => (
                        <Pill
                            key={parsedEmail.id}
                            label={parsedEmail.email}
                            color={parsedEmail.isValid ? 'gray' : 'red'}
                            variant={parsedEmail.isValid ? 'outline' : 'filled'}
                            rightIcon="Close"
                            clickable
                            customStyle={
                                !parsedEmail.isValid
                                    ? {
                                          color: colors.red[1200],
                                      }
                                    : undefined
                            }
                            onClickRightIcon={(e) => {
                                e.stopPropagation();
                                handlePillRemove(index);
                            }}
                        />
                    ))}

                    <HiddenInput
                        ref={inputRef}
                        value={inputValue}
                        onChange={handleInputChange}
                        onKeyDown={handleKeyDown}
                        onBlur={handleBlur}
                        placeholder={emails.length === 0 ? placeholder : ''}
                    />
                </PillsContainer>
            </InputWrapper>
            {currentError ? <ErrorText>{currentError}</ErrorText> : helperText && <HelperText>{helperText}</HelperText>}
        </Container>
    );
}
