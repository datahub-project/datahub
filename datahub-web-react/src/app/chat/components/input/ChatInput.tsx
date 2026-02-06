import { Button, Editor, SimpleSelect, colors } from '@components';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import { SelectOption } from '@components/components/Select/types';

import { ChatPluginsMenu } from '@app/chat/components/input/ChatPluginsMenu';
import { useAppConfig } from '@app/useAppConfig';

const InputContainer = styled.div<{ $isFocused?: boolean; $isWelcomeState?: boolean; $isStreaming?: boolean }>`
    display: flex;
    flex-direction: column;
    flex: 1;
    border: 1px solid
        ${(props) => {
            if (props.$isFocused && !props.$isStreaming) return colors.violet[200];
            return colors.gray[100];
        }};
    border-radius: ${(props) => (props.$isWelcomeState ? '16px' : '12px')};
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
    transition: all 0.2s;
    background-color: white;

    ${(props) => props.$isFocused && !props.$isStreaming && `outline: 1px solid ${colors.violet[200]};`}
`;

const EditorArea = styled.div<{ $isStreaming?: boolean }>`
    flex: 1;
    cursor: ${(props) => (props.$isStreaming ? 'default' : 'text')};
    pointer-events: ${(props) => (props.$isStreaming ? 'none' : 'auto')};
`;

const ControlsRow = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 8px 8px 8px 8px;
    flex-shrink: 0;
`;

const LeftControls = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

interface Props {
    value: string;
    onChange: (value: string) => void;
    onSubmit: () => void;
    onStop?: () => void;
    placeholder?: string;
    isStreaming?: boolean;
    isWelcomeState?: boolean;
    modeOptions?: SelectOption[];
    selectedMode?: string;
    onModeChange?: (mode: string) => void;
    /** Auto-focus the input on mount */
    autoFocus?: boolean;
}

export const ChatInput: React.FC<Props> = ({
    value,
    onChange,
    onSubmit,
    onStop,
    placeholder = 'Type a message...',
    isStreaming = false,
    isWelcomeState = false,
    modeOptions,
    selectedMode,
    onModeChange,
    autoFocus = false,
}) => {
    const { config } = useAppConfig();
    const [isFocused, setIsFocused] = useState(false);
    const editorRef = useRef<any>(null);
    const shouldBlurOnClearRef = useRef(false);
    // Track the last value to detect external clears
    const lastValueRef = useRef(value);

    // Auto-focus input on mount when autoFocus is true
    useEffect(() => {
        if (autoFocus && editorRef.current) {
            requestAnimationFrame(() => {
                editorRef.current?.focus?.();
            });
        }
    }, [autoFocus]);

    // Clear editor content and blur when value is cleared after sending
    useEffect(() => {
        if (value === '' && lastValueRef.current !== '') {
            // Clear the editor content since Editor doesn't sync external content changes in edit mode
            editorRef.current?.commands?.setContent?.('');
            if (shouldBlurOnClearRef.current) {
                editorRef.current?.blur?.();
                shouldBlurOnClearRef.current = false;
            }
        }
        lastValueRef.current = value;
    }, [value]);

    // Blur input when streaming starts
    useEffect(() => {
        if (isStreaming) {
            editorRef.current?.blur?.();
        }
    }, [isStreaming]);

    // Handle submit
    const handleSubmit = useCallback(() => {
        if (!isStreaming && value.trim()) {
            shouldBlurOnClearRef.current = true;
            onSubmit();
        }
    }, [isStreaming, value, onSubmit]);

    const isSubmitDisabled = !isStreaming && !value.trim();

    const handleButtonClick = useCallback(() => {
        if (isStreaming && onStop) {
            onStop();
        } else if (!isStreaming && !isSubmitDisabled) {
            shouldBlurOnClearRef.current = true;
            onSubmit();
        }
    }, [isStreaming, onStop, isSubmitDisabled, onSubmit]);

    const showModeSelect = Boolean(modeOptions?.length && onModeChange);

    // Handle focus/blur events from the editor wrapper
    const handleFocus = useCallback(() => setIsFocused(true), []);
    const handleBlur = useCallback(() => setIsFocused(false), []);

    // Handle Enter key to submit (Shift+Enter for new line)
    const handleKeyDown = useCallback(
        (event: React.KeyboardEvent<HTMLDivElement>) => {
            if (event.key === 'Enter' && !event.shiftKey) {
                // Check if mentions dropdown is active (either in-editor or portal-rendered)
                const mentionsDropdown = document.querySelector(
                    '.remirror-floating-popover, .mentions-dropdown-portal',
                );
                if (mentionsDropdown) {
                    // Let dropdown handle Enter for selection
                    return;
                }

                event.preventDefault();
                handleSubmit();
            }
        },
        [handleSubmit],
    );

    // Handle paste - insert as plain text to avoid paragraph wrapping
    const handlePaste = useCallback((event: React.ClipboardEvent<HTMLDivElement>) => {
        const text = event.clipboardData.getData('text/plain');
        if (text) {
            event.preventDefault();
            editorRef.current?.commands?.insertText?.(text);
            // Scroll the editor container to show the cursor
            requestAnimationFrame(() => {
                const editor = document.querySelector('.remirror-editor.ProseMirror') as HTMLElement;
                if (editor) {
                    editor.scrollTop = editor.scrollHeight;
                }
            });
        }
    }, []);

    return (
        <InputContainer
            $isFocused={isFocused}
            $isWelcomeState={isWelcomeState}
            $isStreaming={isStreaming}
            onFocus={handleFocus}
            onBlur={handleBlur}
        >
            <EditorArea $isStreaming={isStreaming}>
                <Editor
                    ref={editorRef}
                    content={value}
                    onChange={onChange}
                    onKeyDown={handleKeyDown}
                    onPaste={handlePaste}
                    placeholder={placeholder}
                    readOnly={isStreaming}
                    hideToolbar
                    compact
                    hideBorder
                    doNotFocus={!autoFocus}
                />
            </EditorArea>
            <ControlsRow>
                <LeftControls>
                    {showModeSelect && (
                        <SimpleSelect
                            options={modeOptions || []}
                            values={selectedMode ? [selectedMode] : []}
                            onUpdate={(values) => {
                                const nextMode = values[0];
                                if (nextMode) {
                                    onModeChange?.(nextMode);
                                }
                            }}
                            size="sm"
                            width="fit-content"
                            showClear={false}
                            showSearch={false}
                            isDisabled={isStreaming}
                            optionSwitchable={false}
                        />
                    )}
                    {config.featureFlags.aiPluginsEnabled ? <ChatPluginsMenu /> : null}
                </LeftControls>
                <Button
                    onClick={handleButtonClick}
                    disabled={isSubmitDisabled}
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
            </ControlsRow>
        </InputContainer>
    );
};
