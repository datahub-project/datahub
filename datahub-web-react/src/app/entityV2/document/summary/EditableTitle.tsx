import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useDocumentsContext } from '@app/documentV2/DocumentsContext';
import { useDocumentPermissions } from '@app/documentV2/hooks/useDocumentPermissions';
import { useUpdateDocument } from '@app/documentV2/hooks/useUpdateDocument';
import colors from '@src/alchemy-components/theme/foundations/colors';

const TitleContainer = styled.div`
    width: 100%;
`;

const TitleInput = styled.input<{ $editable: boolean }>`
    font-size: 28px;
    font-weight: 600;
    color: ${colors.gray[1700]};
    border: none;
    outline: none;
    background: transparent;
    width: 100%;
    padding: 8px 0;
    cursor: ${(props) => (props.$editable ? 'text' : 'default')};
    border-bottom: 2px solid transparent;
    transition: border-color 0.2s ease;

    &:hover {
        border-bottom-color: ${(props) => (props.$editable ? colors.gray[300] : 'transparent')};
    }

    &:focus {
        border-bottom-color: ${colors.blue[400]};
    }

    &::placeholder {
        color: ${colors.gray[500]};
    }
`;

interface Props {
    documentUrn: string;
    initialTitle: string;
}

export const EditableTitle: React.FC<Props> = ({ documentUrn, initialTitle }) => {
    const [title, setTitle] = useState(initialTitle || 'New Document');
    const [isSaving, setIsSaving] = useState(false);
    const { canEdit } = useDocumentPermissions(documentUrn);
    const { updateContents } = useUpdateDocument();
    const { setUpdatedDocument } = useDocumentsContext();

    useEffect(() => {
        setTitle(initialTitle || 'New Document');
    }, [initialTitle]);

    const handleBlur = async () => {
        if (title !== initialTitle && !isSaving) {
            setIsSaving(true);
            await updateContents({
                urn: documentUrn,
                title,
            });

            // Notify context that document was updated
            setUpdatedDocument({ urn: documentUrn });

            setIsSaving(false);
        }
    };

    const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
        if (e.key === 'Enter') {
            e.currentTarget.blur();
        }
    };

    return (
        <TitleContainer>
            <TitleInput
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                onBlur={handleBlur}
                onKeyDown={handleKeyDown}
                $editable={canEdit}
                disabled={!canEdit}
                placeholder="New Document"
            />
        </TitleContainer>
    );
};
