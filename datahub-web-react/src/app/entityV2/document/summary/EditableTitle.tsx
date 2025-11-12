import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useDocumentPermissions } from '@app/documentV2/hooks/useDocumentPermissions';
import { useUpdateDocumentTitleMutation } from '@app/documentV2/hooks/useDocumentTreeMutations';
import colors from '@src/alchemy-components/theme/foundations/colors';

const TitleContainer = styled.div`
    width: 100%;
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
    const { canEditTitle } = useDocumentPermissions(documentUrn);
    const { updateTitle } = useUpdateDocumentTitleMutation();

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
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                onInput={handleInput}
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
