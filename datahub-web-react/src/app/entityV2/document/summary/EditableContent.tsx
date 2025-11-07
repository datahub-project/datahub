import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { useDocumentPermissions } from '@app/documentV2/hooks/useDocumentPermissions';
import { useExtractMentions } from '@app/documentV2/hooks/useExtractMentions';
import { useUpdateDocument } from '@app/documentV2/hooks/useUpdateDocument';
import { RelatedAssetsSection } from '@app/entityV2/document/summary/RelatedAssetsSection';
import { RelatedDocumentsSection } from '@app/entityV2/document/summary/RelatedDocumentsSection';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { DocumentRelatedAsset, DocumentRelatedDocument } from '@types';

const SectionHeader = styled.h4`
    font-size: 16px;
    font-weight: 600;
    margin: 0;
    color: ${colors.gray[1700]};
`;

const ContentWrapper = styled.div`
    width: 100%;
    display: flex;
    flex-direction: column;
    gap: 24px;
`;

const EditorSection = styled.div`
    width: 100%;
`;

const ContentTextArea = styled.textarea<{ $editable: boolean }>`
    font-size: 15px;
    line-height: 1.7;
    color: ${colors.gray[1700]};
    border: 1px solid transparent;
    outline: none;
    background-color: transparent;
    width: 100%;
    min-height: 400px;
    padding: 16px;
    border-radius: 8px;
    cursor: ${(props) => (props.$editable ? 'text' : 'default')};
    resize: vertical;
    font-family: inherit;
    transition:
        border-color 0.2s ease,
        background-color 0.2s ease;

    &:hover {
        background-color: ${(props) => (props.$editable ? colors.gray[0] : 'transparent')};
        border-color: ${(props) => (props.$editable ? colors.gray[100] : 'transparent')};
    }

    &:focus {
        background-color: ${colors.blue[0]};
        border-color: ${colors.blue[400]};
    }

    &::placeholder {
        color: ${colors.gray[1800]};
    }

    &:disabled {
        cursor: not-allowed;
        color: ${colors.gray[600]};
    }
`;

const SaveIndicator = styled.div`
    font-size: 12px;
    color: ${colors.gray[1800]};
    font-style: italic;
    text-align: right;
    padding-top: 4px;
`;

interface EditableContentProps {
    documentUrn: string;
    initialContent: string;
    relatedAssets?: DocumentRelatedAsset[];
    relatedDocuments?: DocumentRelatedDocument[];
}

export const EditableContent: React.FC<EditableContentProps> = ({
    documentUrn,
    initialContent,
    relatedAssets,
    relatedDocuments,
}) => {
    const [content, setContent] = useState(initialContent || '');
    const [isSaving, setIsSaving] = useState(false);
    const [lastSaved, setLastSaved] = useState<Date | null>(null);
    const { canEdit } = useDocumentPermissions(documentUrn);
    const { updateContents, updateRelatedEntities } = useUpdateDocument();
    const { documentUrns, assetUrns } = useExtractMentions(content);

    useEffect(() => {
        setContent(initialContent || '');
        setLastSaved(new Date()); // Reset last saved on initial content load
    }, [initialContent]);

    // Auto-save after 2 seconds of no typing
    useEffect(() => {
        if (content !== initialContent && canEdit) {
            const timer = setTimeout(async () => {
                setIsSaving(true);

                try {
                    // Save content
                    await updateContents({
                        urn: documentUrn,
                        contents: { text: content },
                    });

                    // Update related entities based on @ mentions
                    await updateRelatedEntities({
                        urn: documentUrn,
                        relatedAssets: assetUrns,
                        relatedDocuments: documentUrns,
                    });

                    setLastSaved(new Date());
                } catch (error) {
                    console.error('Failed to save document:', error);
                } finally {
                    setIsSaving(false);
                }
            }, 2000);

            return () => clearTimeout(timer);
        }
        return undefined;
    }, [content, initialContent, documentUrn, canEdit, updateContents, updateRelatedEntities, documentUrns, assetUrns]);

    return (
        <ContentWrapper>
            <EditorSection>
                <SectionHeader>Content</SectionHeader>
                <ContentTextArea
                    value={content}
                    onChange={(e) => setContent(e.target.value)}
                    $editable={canEdit}
                    disabled={!canEdit}
                    placeholder={canEdit ? 'Start writing your document...' : 'No content'}
                />
                {canEdit && (
                    <SaveIndicator>
                        {isSaving && 'Saving...'}
                        {!isSaving && lastSaved && `Last saved at ${lastSaved.toLocaleTimeString()}`}
                        {!isSaving && !lastSaved && content !== initialContent && 'Unsaved changes'}
                    </SaveIndicator>
                )}
            </EditorSection>

            <RelatedDocumentsSection relatedDocuments={relatedDocuments} />
            <RelatedAssetsSection relatedAssets={relatedAssets} />
        </ContentWrapper>
    );
};
