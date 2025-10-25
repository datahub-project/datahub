import { colors } from '@components';
import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import { ChatMentionsDropdown } from '@app/chat/components/input/ChatMentionsDropdown';
import { useMentionInput } from '@app/chat/hooks/useMentionInput';
import { flattenAutocompleteSuggestions } from '@app/chat/utils/autocompleteUtils';
import { useUserContext } from '@app/context/useUserContext';

import { useGetAutoCompleteMultipleResultsLazyQuery } from '@graphql/search.generated';

const InputContainer = styled.div`
    position: relative;
    flex: 1;
`;

const ContentEditableDiv = styled.div<{ $isFocused?: boolean; $disabled?: boolean }>`
    width: 100%;
    min-height: 40px;
    max-height: 84px; /* 3 lines: (3 * 20px line-height) + (12px * 2 padding) = 84px */
    padding: 12px 16px;
    border: 1px solid
        ${(props) => {
            if (props.$isFocused) return colors.violet[200];
            return colors.gray[100];
        }};
    border-radius: 8px;
    font-size: 14px;
    font-family: inherit;
    line-height: 20px;
    overflow-y: auto;
    overflow-x: hidden;
    overflow-wrap: break-word;
    word-break: break-word;
    white-space: pre-wrap;
    outline: none;
    transition: all 0.2s;
    background-color: white;
    cursor: text;

    ${(props) => props.$isFocused && `outline: 1px solid ${colors.violet[200]};`}

    ${(props) =>
        props.$disabled &&
        `
        background-color: ${colors.gray[1500]};
        cursor: not-allowed;
        opacity: 0.6;
        pointer-events: none;
    `}

    &:empty:before {
        content: attr(data-placeholder);
        color: ${colors.gray[400]};
        pointer-events: none;
    }

    /* Mention spans */
    .mention {
        color: ${colors.primary[500]};
        font-weight: 500;
        cursor: pointer;

        &:hover {
            text-decoration: underline;
        }
    }

    /* Only show scrollbar when content exceeds max-height */
    &::-webkit-scrollbar {
        width: 4px;
    }

    &::-webkit-scrollbar-thumb {
        background-color: ${colors.gray[300]};
        border-radius: 2px;
    }
`;

interface Props {
    value: string;
    onChange: (value: string) => void;
    onSubmit: () => void;
    placeholder?: string;
    disabled?: boolean;
}

export const ChatInput: React.FC<Props> = ({
    value,
    onChange,
    onSubmit,
    placeholder = 'Type a message...',
    disabled = false,
}) => {
    const [isFocused, setIsFocused] = useState(false);
    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn;
    const [getAutoComplete, { data: autocompleteData, loading }] = useGetAutoCompleteMultipleResultsLazyQuery();

    // Use the mention input hook
    const {
        contentEditableRef,
        mentionState,
        handleInput,
        handleEntitySelect,
        handleKeyDown: handleMentionKeyDown,
        handleBlur,
    } = useMentionInput({
        value,
        onChange,
        onEntitySelect: () => {}, // No additional action needed
    });

    // Fetch autocomplete suggestions when query changes
    useEffect(() => {
        if (mentionState.isActive && mentionState.query) {
            getAutoComplete({ variables: { input: { query: mentionState.query, viewUrn } } });
        }
    }, [mentionState.isActive, mentionState.query, getAutoComplete, viewUrn]);

    // Handle keyboard events including submit
    const handleKeyDown = useCallback(
        (e: React.KeyboardEvent<HTMLDivElement>) => {
            // Handle mention-specific keyboard events
            handleMentionKeyDown(e);

            // Handle submit
            if (!mentionState.isActive && e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                if (!disabled && value.trim()) {
                    onSubmit();
                }
            }
        },
        [handleMentionKeyDown, mentionState.isActive, disabled, value, onSubmit],
    );

    const suggestions = autocompleteData?.autoCompleteForMultiple?.suggestions || [];
    const entities = flattenAutocompleteSuggestions(suggestions);

    return (
        <InputContainer>
            <ContentEditableDiv
                ref={contentEditableRef}
                contentEditable={!disabled}
                onInput={handleInput}
                onKeyDown={handleKeyDown}
                onFocus={() => setIsFocused(true)}
                onBlur={() => {
                    setIsFocused(false);
                    handleBlur();
                }}
                data-placeholder={placeholder}
                $isFocused={isFocused}
                $disabled={disabled}
                suppressContentEditableWarning
            />
            {mentionState.isActive && (
                <ChatMentionsDropdown
                    query={mentionState.query}
                    entities={entities}
                    loading={loading}
                    onSelect={handleEntitySelect}
                    coordinates={mentionState.coordinates}
                />
            )}
        </InputContainer>
    );
};
