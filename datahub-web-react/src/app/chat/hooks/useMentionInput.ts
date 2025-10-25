import { useCallback, useEffect, useRef, useState } from 'react';

import { htmlToMarkdown, markdownToHtml } from '@app/chat/utils/markdownUtils';
import type { MentionState } from '@app/chat/utils/mentionUtils';

import { Entity } from '@types';

interface UseMentionInputProps {
    value: string;
    onChange: (value: string) => void;
    onEntitySelect: (entity: Entity, displayName: string) => void;
}

export function useMentionInput({ value, onChange, onEntitySelect }: UseMentionInputProps) {
    const contentEditableRef = useRef<HTMLDivElement>(null);
    const [mentionState, setMentionState] = useState<MentionState>({
        isActive: false,
        query: '',
        startIndex: -1,
        coordinates: { top: 0, left: 0 },
    });

    // Update content when value changes externally (like after submit)
    useEffect(() => {
        if (contentEditableRef.current) {
            const nextHtml = markdownToHtml(value);
            if (contentEditableRef.current.innerHTML !== nextHtml) {
                contentEditableRef.current.innerHTML = nextHtml;
            }
        }
    }, [value]);

    // Calculate dropdown position
    const updateDropdownPosition = useCallback(() => {
        if (!contentEditableRef.current) return { top: 0, left: 0 };

        const rect = contentEditableRef.current.getBoundingClientRect();
        const { top: rectTop, left: rectLeft } = rect;

        // Position 8px above the top of the input
        const top = rectTop - 8;
        const left = rectLeft;

        return { top, left };
    }, []);

    // Keep a live Range for the active mention; avoids HTML/markdown index mismatch
    const mentionRangeRef = useRef<Range | null>(null);

    // Handle input changes - detect mentions in the CURRENT text node only
    const handleInput = useCallback(() => {
        const root = contentEditableRef.current;
        if (!root) return;

        const selection = document.getSelection();
        if (!selection || selection.rangeCount === 0) {
            if (mentionState.isActive) {
                setMentionState({ isActive: false, query: '', startIndex: -1, coordinates: { top: 0, left: 0 } });
                mentionRangeRef.current = null;
            }
            // Still emit latest text to keep parent value in sync
            onChange(htmlToMarkdown(root.innerHTML));
            return;
        }

        const range = selection.getRangeAt(0);
        const focusNode = range.startContainer;
        const focusOffset = range.startOffset;

        if (focusNode.nodeType !== Node.TEXT_NODE) {
            if (mentionState.isActive) {
                setMentionState({ isActive: false, query: '', startIndex: -1, coordinates: { top: 0, left: 0 } });
                mentionRangeRef.current = null;
            }
            // Still emit latest text to keep parent value in sync
            onChange(htmlToMarkdown(root.innerHTML));
            return;
        }

        const text = (focusNode as Text).data;
        const uptoCursor = text.slice(0, Math.max(0, focusOffset));
        const atIndexInNode = uptoCursor.lastIndexOf('@');

        if (atIndexInNode === -1) {
            if (mentionState.isActive) {
                setMentionState({ isActive: false, query: '', startIndex: -1, coordinates: { top: 0, left: 0 } });
                mentionRangeRef.current = null;
            }
            // Still emit latest text to keep parent value in sync
            onChange(htmlToMarkdown(root.innerHTML));
            return;
        }

        // Ensure there's no whitespace between @ and cursor
        const query = uptoCursor.slice(atIndexInNode + 1);
        if (/\s/.test(query)) {
            if (mentionState.isActive) {
                setMentionState({ isActive: false, query: '', startIndex: -1, coordinates: { top: 0, left: 0 } });
                mentionRangeRef.current = null;
            }
            return;
        }

        // Create and store a range that spans @... up to the caret
        const mentionRange = document.createRange();
        mentionRange.setStart(focusNode, atIndexInNode);
        mentionRange.setEnd(focusNode, focusOffset);
        mentionRangeRef.current = mentionRange;

        const coordinates = updateDropdownPosition();
        setMentionState({
            isActive: true,
            query,
            // Provide a character index solely for legacy consumers; it's not used for replacement anymore
            startIndex: atIndexInNode,
            coordinates,
        });
        // Emit latest markdown on every input to keep external state up-to-date
        onChange(htmlToMarkdown(root.innerHTML));
    }, [mentionState.isActive, updateDropdownPosition, onChange]);

    // Handle entity selection from dropdown - replace DOM range, then emit markdown
    const handleEntitySelect = useCallback(
        (entity: Entity, displayName: string) => {
            const root = contentEditableRef.current;
            if (!root) return;

            const activeRange = mentionRangeRef.current;
            if (!activeRange) return;

            // Build a non-editable mention chip
            const chip = document.createElement('span');
            chip.className = 'mention';
            chip.setAttribute('data-urn', entity.urn);
            chip.setAttribute('contenteditable', 'false');
            chip.textContent = `@${displayName}`;

            // Replace the @query with the chip
            activeRange.deleteContents();
            activeRange.insertNode(chip);

            // Insert a trailing space and place caret after it
            const space = document.createTextNode(' ');
            chip.after(space);

            const selection = document.getSelection();
            if (selection) {
                const afterRange = document.createRange();
                afterRange.setStart(space, 1);
                afterRange.collapse(true);
                selection.removeAllRanges();
                selection.addRange(afterRange);
            }

            // Emit updated markdown and notify selection
            const newMarkdown = htmlToMarkdown(root.innerHTML);
            onChange(newMarkdown);
            onEntitySelect(entity, displayName);

            // Clear mention state
            mentionRangeRef.current = null;
            setMentionState({ isActive: false, query: '', startIndex: -1, coordinates: { top: 0, left: 0 } });
        },
        [onChange, onEntitySelect],
    );

    // Handle keyboard events
    const handleKeyDown = useCallback(
        (e: React.KeyboardEvent<HTMLDivElement>) => {
            // If dropdown is open, let it handle arrow keys and enter
            if (mentionState.isActive) {
                if (e.key === 'Escape') {
                    setMentionState({
                        isActive: false,
                        query: '',
                        startIndex: -1,
                        coordinates: { top: 0, left: 0 },
                    });
                    e.preventDefault();
                    return;
                }
                // Don't submit on Enter when dropdown is active - let dropdown handle it
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                }
            }
        },
        [mentionState.isActive],
    );

    // Handle when user finishes typing (like on blur or submit)
    const handleBlur = useCallback(() => {
        if (!contentEditableRef.current) return;

        // Convert current HTML to markdown and call onChange
        const currentMarkdown = htmlToMarkdown(contentEditableRef.current.innerHTML);
        onChange(currentMarkdown);
    }, [onChange]);

    return {
        contentEditableRef,
        mentionState,
        handleInput,
        handleEntitySelect,
        handleKeyDown,
        handleBlur,
        setMentionState,
    };
}
