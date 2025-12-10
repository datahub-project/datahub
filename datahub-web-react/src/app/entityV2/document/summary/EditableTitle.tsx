import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';

import { useDocumentPermissions } from '@app/document/hooks/useDocumentPermissions';
import { useUpdateDocumentTitleMutation } from '@app/document/hooks/useDocumentTreeMutations';
import colors from '@src/alchemy-components/theme/foundations/colors';

const TitleContainer = styled.div`
    width: 100%;
    min-width: 0;
`;

const TitleInput = styled.textarea<{ $editable: boolean }>`
    font-size: 32px;
    font-weight: 700;
    line-height: 1.4;
    color: ${colors.gray[600]};
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
    overflow-y: auto;
    overflow-x: hidden;
    font-family: inherit;
    white-space: pre-wrap;
    word-wrap: break-word;
    overflow-wrap: break-word;
    box-sizing: border-box;
    min-height: calc(32px * 1.4 + 12px); /* 1 row: font-size * line-height + padding */
    max-height: calc(32px * 1.4 * 3 + 12px); /* 3 rows: font-size * line-height * 3 + padding */
    &:hover {
        background-color: transparent;
    }

    &:focus {
        background-color: transparent;
    }

    &::placeholder {
        color: ${colors.gray[400]};
        opacity: 0.4;
    }
`;

interface Props {
    documentUrn: string;
    initialTitle: string;
}

export const EditableTitle: React.FC<Props> = ({ documentUrn, initialTitle }) => {
    const [title, setTitle] = useState(initialTitle || '');
    const [isSaving, setIsSaving] = useState(false);
    const textareaRef = useRef<HTMLTextAreaElement>(null);
    const { canEditTitle } = useDocumentPermissions(documentUrn);
    const { updateTitle } = useUpdateDocumentTitleMutation();

    useEffect(() => {
        setTitle(initialTitle || '');
    }, [initialTitle]);

    // Auto-resize textarea up to 3 rows, then scroll
    useEffect(() => {
        const textarea = textareaRef.current;
        if (!textarea) return;

        const { style, scrollHeight } = textarea;

        // Reset height to auto to get the correct scrollHeight
        style.height = 'auto';

        // Calculate max height for 3 rows (font-size * line-height * 3 + padding)
        const maxHeight = 32 * 1.4 * 3 + 12; // ~146px

        // Set height to scrollHeight, but cap at maxHeight
        style.height = `${Math.min(scrollHeight, maxHeight)}px`;
    }, [title]);

    const handleBlur = async () => {
        if (title !== initialTitle && !isSaving) {
            setIsSaving(true);

            // Tree mutation handles optimistic update + backend call + rollback on error!
            await updateTitle(documentUrn, title);

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
                placeholder="New Document"
                rows={1}
            />
        </TitleContainer>
    );
};
