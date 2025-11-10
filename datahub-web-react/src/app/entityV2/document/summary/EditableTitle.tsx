import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useDocumentsContext } from '@app/documentV2/DocumentsContext';
import { useDocumentPermissions } from '@app/documentV2/hooks/useDocumentPermissions';
import { useUpdateDocument } from '@app/documentV2/hooks/useUpdateDocument';
import colors from '@src/alchemy-components/theme/foundations/colors';

const TitleContainer = styled.div`
    width: 100%;
`;

const TitleInput = styled.textarea<{ $editable: boolean }>`
    font-size: 32px;
    font-weight: 700;
    line-height: 1.4;
    color: ${colors.gray[1700]};
    border: none;
    outline: none;
    background: transparent;
    width: 100%;
    padding: 6px 8px;
    margin: -6px -8px;
    cursor: ${(props) => (props.$editable ? 'text' : 'default')};
    border-radius: 4px;
    resize: none;
    overflow: hidden;
    font-family: inherit;
    white-space: pre-wrap;
    word-wrap: break-word;

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
    const { canEdit } = useDocumentPermissions(documentUrn);
    const { updateContents } = useUpdateDocument();
    const { setUpdatedDocument } = useDocumentsContext();

    useEffect(() => {
        setTitle(initialTitle || '');
    }, [initialTitle]);

    // Auto-resize textarea to fit content
    const handleInput = (e: React.FormEvent<HTMLTextAreaElement>) => {
        const target = e.currentTarget;
        target.style.height = 'auto';
        target.style.height = `${target.scrollHeight}px`;
    };

    const handleBlur = async () => {
        console.log('[EditableTitle] handleBlur called', {
            title,
            initialTitle,
            titleChanged: title !== initialTitle,
            isSaving,
        });

        if (title !== initialTitle && !isSaving) {
            console.log('[EditableTitle] Saving title...');
            setIsSaving(true);

            // Optimistically update the sidebar immediately
            setUpdatedDocument({ urn: documentUrn, title });

            try {
                await updateContents({
                    urn: documentUrn,
                    title,
                });

                console.log('[EditableTitle] Title saved successfully');
            } catch (error) {
                console.error('[EditableTitle] Failed to save title:', error);
                // Revert optimistic update on error
                setUpdatedDocument({ urn: documentUrn, title: initialTitle });
            } finally {
                setIsSaving(false);
            }
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
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                onInput={handleInput}
                onBlur={handleBlur}
                onKeyDown={handleKeyDown}
                $editable={canEdit}
                disabled={!canEdit}
                placeholder="New Document"
                rows={1}
            />
        </TitleContainer>
    );
};
