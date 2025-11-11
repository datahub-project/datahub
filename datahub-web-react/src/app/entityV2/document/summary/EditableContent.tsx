import { Editor } from '@components';
import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import { useDocumentPermissions } from '@app/documentV2/hooks/useDocumentPermissions';
import { useExtractMentions } from '@app/documentV2/hooks/useExtractMentions';
import { useUpdateDocument } from '@app/documentV2/hooks/useUpdateDocument';
import { RelatedAssetsSection } from '@app/entityV2/document/summary/RelatedAssetsSection';
import { RelatedDocumentsSection } from '@app/entityV2/document/summary/RelatedDocumentsSection';
import useFileUpload from '@app/shared/hooks/useFileUpload';
import useFileUploadAnalyticsCallbacks from '@app/shared/hooks/useFileUploadAnalyticsCallbacks';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { DocumentRelatedAsset, DocumentRelatedDocument, UploadDownloadScenario } from '@types';

const ContentWrapper = styled.div`
    width: 100%;
    display: flex;
    flex-direction: column;
    gap: 24px;
`;

const EditorSection = styled.div`
    width: 100%;
`;

const StyledEditor = styled(Editor)`
    border: none;
    &&& {
        .remirror-editor {
            padding: 0px 0;
            min-height: 400px;
        }
        .remirror-editor.ProseMirror {
            font-size: 15px;
            line-height: 1.7;
            color: ${colors.gray[1700]};
        }
        p:last-of-type {
            margin-bottom: 0;
        }
    }
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
    const [editorKey, setEditorKey] = useState(Date.now());
    const { canEdit } = useDocumentPermissions(documentUrn);
    const { updateContents, updateRelatedEntities } = useUpdateDocument();
    // Extract mentions from content (currently unused, but hook needs to run)
    useExtractMentions(content);

    const uploadFileAnalyticsCallbacks = useFileUploadAnalyticsCallbacks({
        scenario: UploadDownloadScenario.AssetDocumentation,
        assetUrn: documentUrn,
    });

    const { uploadFile } = useFileUpload({
        scenario: UploadDownloadScenario.AssetDocumentation,
        assetUrn: documentUrn,
    });

    useEffect(() => {
        setContent(initialContent || '');
        // Force editor to remount with new content by updating the key
        setEditorKey(Date.now());
    }, [initialContent]);

    // Save function that can be reused
    const saveDocument = useCallback(
        async (contentToSave: string) => {
            if (isSaving || contentToSave === initialContent || !canEdit) {
                return;
            }

            setIsSaving(true);
            try {
                // Extract mentions from the content to save
                const urnPattern = /@(urn:li:[a-zA-Z]+:[^\s)}\]]+)/g;
                const matches = Array.from(contentToSave.matchAll(urnPattern));
                const documentUrnsToSave: string[] = [];
                const assetUrnsToSave: string[] = [];

                matches.forEach((match) => {
                    const urn = match[1];
                    if (urn.includes(':document:')) {
                        if (!documentUrnsToSave.includes(urn)) {
                            documentUrnsToSave.push(urn);
                        }
                    } else if (!assetUrnsToSave.includes(urn)) {
                        assetUrnsToSave.push(urn);
                    }
                });

                // Save content
                await updateContents({
                    urn: documentUrn,
                    contents: { text: contentToSave },
                });

                // Update related entities based on @ mentions
                await updateRelatedEntities({
                    urn: documentUrn,
                    relatedAssets: assetUrnsToSave,
                    relatedDocuments: documentUrnsToSave,
                });
            } catch (error) {
                console.error('[EditableContent] Failed to save document:', error);
            } finally {
                setIsSaving(false);
            }
        },
        [isSaving, initialContent, canEdit, updateContents, updateRelatedEntities, documentUrn],
    );

    // Auto-save after 2 seconds of no typing
    useEffect(() => {
        if (content !== initialContent && canEdit && !isSaving) {
            const timer = setTimeout(() => {
                saveDocument(content);
            }, 2000);

            return () => clearTimeout(timer);
        }
        return undefined;
    }, [content, initialContent, canEdit, isSaving, saveDocument]);

    // Save on blur (clicking away from the editor)
    const handleBlur = useCallback(() => {
        if (content !== initialContent) {
            saveDocument(content);
        }
    }, [content, initialContent, saveDocument]);

    // Save before navigating away
    useEffect(() => {
        const handleBeforeUnload = (e: BeforeUnloadEvent) => {
            if (content !== initialContent && canEdit && !isSaving) {
                // Attempt to save synchronously
                saveDocument(content);

                // Show browser warning if there are unsaved changes
                e.preventDefault();
                e.returnValue = '';
            }
        };

        window.addEventListener('beforeunload', handleBeforeUnload);
        return () => window.removeEventListener('beforeunload', handleBeforeUnload);
    }, [content, initialContent, canEdit, isSaving, saveDocument]);

    return (
        <ContentWrapper>
            <EditorSection onBlur={handleBlur}>
                {canEdit ? (
                    <StyledEditor
                        key={`editor-${editorKey}`}
                        content={content}
                        onChange={setContent}
                        placeholder="Write about anything..."
                        hideBorder
                        fixedBottomToolbar
                        uploadFile={uploadFile}
                        {...uploadFileAnalyticsCallbacks}
                    />
                ) : (
                    <StyledEditor
                        key={`editor-readonly-${editorKey}`}
                        content={content}
                        readOnly
                        placeholder="No content"
                        hideBorder
                    />
                )}
            </EditorSection>

            <RelatedDocumentsSection relatedDocuments={relatedDocuments} />
            <RelatedAssetsSection relatedAssets={relatedAssets} />
        </ContentWrapper>
    );
};
