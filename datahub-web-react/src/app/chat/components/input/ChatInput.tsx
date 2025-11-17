import { Button, colors } from '@components';
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

const ContentEditableDiv = styled.div<{ $isFocused?: boolean; $isWelcomeState?: boolean; $isStreaming?: boolean }>`
    width: 100%;
    min-height: ${(props) => (props.$isWelcomeState ? '120px' : '44px')};
    max-height: 120px; /* Grows up to 120px, then scrolls */
    padding: 12px 56px 12px 16px; /* Extra right padding for send button */
    border: 1px solid
        ${(props) => {
            if (props.$isFocused && !props.$isStreaming) return colors.violet[200];
            return colors.gray[100];
        }};
    border-radius: ${(props) => (props.$isWelcomeState ? '16px' : '12px')};
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
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
    cursor: ${(props) => (props.$isStreaming ? 'default' : 'text')};
    pointer-events: ${(props) => (props.$isStreaming ? 'none' : 'auto')};

    ${(props) => props.$isFocused && !props.$isStreaming && `outline: 1px solid ${colors.violet[200]};`}

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

const SendButtonWrapper = styled.div`
    position: absolute;
    right: 8px;
    bottom: 8px;
    cursor: pointer;
    pointer-events: auto;
`;

interface Props {
    value: string;
    onChange: (value: string) => void;
    onSubmit: () => void;
    onStop?: () => void;
    placeholder?: string;
    isStreaming?: boolean;
    isWelcomeState?: boolean;
}

export const ChatInput: React.FC<Props> = ({
    value,
    onChange,
    onSubmit,
    onStop,
    placeholder = 'Type a message...',
    isStreaming = false,
    isWelcomeState = false,
}) => {
    const [isFocused, setIsFocused] = useState(false);
    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn;
    const [getAutoComplete, { data: autocompleteData, loading }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const shouldBlurOnClearRef = React.useRef(false);

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

    // Blur input when value is cleared after sending
    useEffect(() => {
        if (value === '' && shouldBlurOnClearRef.current && contentEditableRef.current === document.activeElement) {
            contentEditableRef.current?.blur();
            shouldBlurOnClearRef.current = false;
        }
    }, [value, contentEditableRef]);

    // Blur input when streaming starts
    useEffect(() => {
        if (isStreaming && contentEditableRef.current === document.activeElement) {
            contentEditableRef.current?.blur();
        }
    }, [isStreaming, contentEditableRef]);

    // Handle keyboard events including submit
    const handleKeyDown = useCallback(
        (e: React.KeyboardEvent<HTMLDivElement>) => {
            // Handle mention-specific keyboard events
            handleMentionKeyDown(e);

            // Handle submit
            if (!mentionState.isActive && e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                if (!isStreaming && value.trim()) {
                    shouldBlurOnClearRef.current = true;
                    onSubmit();
                }
            }
        },
        [handleMentionKeyDown, mentionState.isActive, isStreaming, value, onSubmit],
    );

    const suggestions = autocompleteData?.autoCompleteForMultiple?.suggestions || [];
    const entities = flattenAutocompleteSuggestions(suggestions);

    const isSubmitDisabled = !isStreaming && !value.trim();

    const handleButtonClick = useCallback(() => {
        if (isStreaming && onStop) {
            onStop();
        } else if (!isStreaming && !isSubmitDisabled) {
            shouldBlurOnClearRef.current = true;
            onSubmit();
        }
    }, [isStreaming, onStop, isSubmitDisabled, onSubmit]);

    return (
        <InputContainer>
            <ContentEditableDiv
                ref={contentEditableRef}
                contentEditable={!isStreaming}
                onInput={handleInput}
                onKeyDown={handleKeyDown}
                onFocus={() => setIsFocused(true)}
                onBlur={() => {
                    setIsFocused(false);
                    handleBlur();
                }}
                data-placeholder={placeholder}
                $isFocused={isFocused}
                $isWelcomeState={isWelcomeState}
                $isStreaming={isStreaming}
                suppressContentEditableWarning
            />
            <SendButtonWrapper>
                <Button
                    onClick={handleButtonClick}
                    isDisabled={isSubmitDisabled}
                    isCircle
                    icon={{
                        icon: isStreaming ? 'Stop' : 'PaperPlaneRight',
                        source: 'phosphor',
                        weight: 'fill',
                    }}
                    size="md"
                    variant="filled"
                    color="violet"
                    aria-label={isStreaming ? 'Stop generating' : 'Send message'}
                />
            </SendButtonWrapper>
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
