import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useDocumentPermissions } from '@app/document/hooks/useDocumentPermissions';
import { useUpdateDocumentTitleMutation } from '@app/document/hooks/useDocumentTreeMutations';
import { DEFAULT_DOCUMENT_TITLE } from '@app/document/utils/documentTreeNodeMerge';

const TitleContainer = styled.div`
    width: 100%;
    min-width: 0;
`;

const TitleInput = styled.textarea<{ $editable: boolean }>`
    font-size: 32px;
    font-weight: 700;
    line-height: 1.4;
    color: ${(props) => props.theme.colors.text};
    border: none;
    outline: none;
    background: transparent;
    width: 100%;
    min-width: 0;
    padding: 6px 8px;
    margin: -6px -8px;
    cursor: ${(props) => (props.$editable ? 'text' : 'default')};
    border-radius: 4px;
    resize: none;
    overflow: hidden;
    font-family: inherit;
    white-space: pre-wrap;
    word-wrap: break-word;
    overflow-wrap: break-word;
    box-sizing: border-box;
    min-height: calc(32px * 1.4 + 12px); /* 1 row: font-size * line-height + padding */
    &:hover {
        background-color: transparent;
    }

    &:focus {
        background-color: transparent;
    }

    &::placeholder {
        color: ${(props) => props.theme.colors.textTertiary};
        opacity: 0.4;
    }
`;

interface Props {
    documentUrn: string;
    initialTitle: string;
}

export const EditableTitle: React.FC<Props> = ({ documentUrn, initialTitle }) => {
    const { t } = useTranslation('entity.types');
    const [title, setTitle] = useState(initialTitle || '');
    const [isSaving, setIsSaving] = useState(false);
    const textareaRef = useRef<HTMLTextAreaElement>(null);
    const hasAutoFocused = useRef(false);
    const { canEditTitle } = useDocumentPermissions(documentUrn);
    const { updateTitle } = useUpdateDocumentTitleMutation();

    useEffect(() => {
        setTitle(initialTitle || '');
    }, [initialTitle]);

    // For freshly created docs, clear the default title and focus the field so the placeholder shows and typing starts immediately.
    // Compare against the persisted English default, not the translated placeholder: documents are created with
    // DEFAULT_DOCUMENT_TITLE regardless of locale.
    useEffect(() => {
        const trimmed = (initialTitle || '').trim().toLowerCase();
        const isDefaultTitle = trimmed === DEFAULT_DOCUMENT_TITLE.toLowerCase() || trimmed === '';

        if (canEditTitle && isDefaultTitle && !hasAutoFocused.current) {
            hasAutoFocused.current = true;
            setTitle('');
            // Defer focus to next paint so the DOM is ready.
            requestAnimationFrame(() => {
                textareaRef.current?.focus();
            });
        }
    }, [canEditTitle, initialTitle]);

    // Two effects so we don't tear down and recreate the ResizeObserver on
    // every keystroke:
    //   1. Recompute height whenever the title changes (content drives the
    //      required height — the observer alone won't catch this since the
    //      textarea's box doesn't change until we set its height).
    //   2. Subscribe once for width changes (e.g. a page scrollbar appears
    //      or disappears, narrowing the available width and forcing wrap).
    useEffect(() => {
        const textarea = textareaRef.current;
        if (!textarea) return;
        textarea.style.height = 'auto';
        textarea.style.height = `${textarea.scrollHeight}px`;
    }, [title]);

    useEffect(() => {
        const textarea = textareaRef.current;
        if (!textarea) return undefined;

        const observer = new ResizeObserver(() => {
            textarea.style.height = 'auto';
            textarea.style.height = `${textarea.scrollHeight}px`;
        });
        observer.observe(textarea);
        return () => observer.disconnect();
    }, []);

    const handleBlur = async () => {
        // If the user leaves the field empty, fall back to the default placeholder title.
        const trimmed = title.trim();
        const fallbackTitle = initialTitle || DEFAULT_DOCUMENT_TITLE;
        const finalTitle = trimmed === '' ? fallbackTitle : title;

        if (finalTitle !== title) {
            setTitle(finalTitle);
        }

        if (finalTitle !== initialTitle && !isSaving) {
            setIsSaving(true);

            // Tree mutation handles optimistic update + backend call + rollback on error!
            await updateTitle(documentUrn, finalTitle);

            setIsSaving(false);
        }
    };

    const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            e.currentTarget.blur();
        }
    };

    return (
        <TitleContainer>
            <TitleInput
                ref={textareaRef}
                data-testid="document-title-input"
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                onBlur={handleBlur}
                onKeyDown={handleKeyDown}
                $editable={canEditTitle}
                disabled={!canEditTitle}
                placeholder={t('document.newDocumentPlaceholder')}
                rows={1}
            />
        </TitleContainer>
    );
};
