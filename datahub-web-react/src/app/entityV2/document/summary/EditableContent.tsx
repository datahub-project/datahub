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
    }, [initialContent]);

    // Save function that can be reused
    const saveDocument = useCallback(
        async (contentToSave: string) => {
            console.log('[EditableContent] saveDocument called', {
                contentToSave: contentToSave.substring(0, 50),
                initialContent: initialContent.substring(0, 50),
                isSaving,
                canEdit,
            });

            if (isSaving || contentToSave === initialContent || !canEdit) {
                console.log('[EditableContent] Skipping save:', {
                    isSaving,
                    sameContent: contentToSave === initialContent,
                    canEdit,
                });
                return;
            }

            console.log('[EditableContent] Starting save...');
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
                console.log('[EditableContent] Calling updateContents mutation...');
                await updateContents({
                    urn: documentUrn,
                    contents: { text: contentToSave },
                });

                console.log('[EditableContent] Content saved successfully, updating related entities...');
                // Update related entities based on @ mentions
                await updateRelatedEntities({
                    urn: documentUrn,
                    relatedAssets: assetUrnsToSave,
                    relatedDocuments: documentUrnsToSave,
                });

                console.log('[EditableContent] Save complete!');
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
        console.log('[EditableContent] Auto-save effect triggered', {
            contentChanged: content !== initialContent,
            canEdit,
            isSaving,
        });

        if (content !== initialContent && canEdit && !isSaving) {
            console.log('[EditableContent] Setting 2-second timer for auto-save...');
            const timer = setTimeout(() => {
                console.log('[EditableContent] Timer fired, calling saveDocument...');
                saveDocument(content);
            }, 2000);

            return () => {
                console.log('[EditableContent] Clearing timer');
                clearTimeout(timer);
            };
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
                        content={content}
                        onChange={setContent}
                        placeholder="Write about anything..."
                        hideBorder
                        fixedBottomToolbar
                        uploadFile={uploadFile}
                        {...uploadFileAnalyticsCallbacks}
                    />
                ) : (
                    <StyledEditor content={content} readOnly placeholder="No content" hideBorder />
                )}
            </EditorSection>

            <RelatedDocumentsSection relatedDocuments={relatedDocuments} />
            <RelatedAssetsSection relatedAssets={relatedAssets} />
        </ContentWrapper>
    );
};
