import { Editor } from '@components';
import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import { useDocumentPermissions } from '@app/documentV2/hooks/useDocumentPermissions';
import { useExtractMentions } from '@app/documentV2/hooks/useExtractMentions';
import { useUpdateDocument } from '@app/documentV2/hooks/useUpdateDocument';
import { useRefetch } from '@app/entity/shared/EntityContext';
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
    position: relative;
`;

const StyledEditor = styled(Editor)<{ $hideToolbar?: boolean }>`
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

    /* Hide toolbar completely when not focused - must come before animations */
    ${(props) =>
        props.$hideToolbar &&
        `
        .remirror-theme > div:first-child,
        .remirror-editor-wrapper > div:first-child:not(.remirror-editor) {
            display: none !important;
        }
        padding-bottom: 0 !important;
    `}

    /* Animate toolbar appearance - only on show, not hide */
    ${(props) =>
        !props.$hideToolbar &&
        `
        .remirror-theme > div:first-child,
        .remirror-editor-wrapper > div:first-child:not(.remirror-editor) {
            animation: fadeIn 0.3s ease-out;
        }
    `}

    @keyframes fadeIn {
        from {
            opacity: 0;
        }
        to {
            opacity: 1;
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
    const [isEditorFocused, setIsEditorFocused] = useState(false);
    const [editorVersion, setEditorVersion] = useState(0);
    const lastSavedContentRef = React.useRef<string>(initialContent || '');
    const { canEditContents } = useDocumentPermissions(documentUrn);
    const { updateContents, updateRelatedEntities } = useUpdateDocument();
    const refetch = useRefetch();
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

    // Detect when initialContent changes externally (e.g., version restore, not from our save)
    useEffect(() => {
        const newContent = initialContent || '';

        // If content changed and it's NOT from our own save, increment version to remount editor
        if (newContent !== lastSavedContentRef.current && newContent !== content) {
            setContent(newContent);
            setEditorVersion((v) => v + 1);
        }

        // Update the ref to track the latest server content
        lastSavedContentRef.current = newContent;
    }, [initialContent, content]);

    // Save function that can be reused
    const saveDocument = useCallback(
        async (contentToSave: string) => {
            if (isSaving || contentToSave === initialContent || !canEditContents) {
                return;
            }

            setIsSaving(true);
            try {
                // Extract mentions from the content to save
                // Pattern matches markdown link syntax: [text](urn:li:entityType:id)
                const urnPattern = /\[([^\]]+)\]\((urn:li:[a-zA-Z]+:[^\s)]+)\)/g;
                const matches = Array.from(contentToSave.matchAll(urnPattern));
                const documentUrnsToSave: string[] = [];
                const assetUrnsToSave: string[] = [];

                matches.forEach((match) => {
                    const urn = match[2]; // URN is in the second capture group
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

                // Track that we just saved this content to prevent remount on refetch
                lastSavedContentRef.current = contentToSave;

                // Refetch the document to get the updated related assets/documents
                await refetch();
            } catch (error) {
                console.error('[EditableContent] Failed to save document:', error);
            } finally {
                setIsSaving(false);
            }
        },
        [isSaving, initialContent, canEditContents, updateContents, updateRelatedEntities, documentUrn, refetch],
    );

    // Auto-save after 2 seconds of no typing
    useEffect(() => {
        if (content !== initialContent && canEditContents && !isSaving) {
            const timer = setTimeout(() => {
                saveDocument(content);
            }, 3000);

            return () => clearTimeout(timer);
        }
        return undefined;
    }, [content, initialContent, canEditContents, isSaving, saveDocument]);

    // Save on blur (clicking away from the editor)
    const handleBlur = useCallback(() => {
        if (content !== initialContent) {
            saveDocument(content);
        }
    }, [content, initialContent, saveDocument]);

    // Save before navigating away
    useEffect(() => {
        const handleBeforeUnload = (e: BeforeUnloadEvent) => {
            if (content !== initialContent && canEditContents && !isSaving) {
                // Attempt to save synchronously
                saveDocument(content);

                // Show browser warning if there are unsaved changes
                e.preventDefault();
                e.returnValue = '';
            }
        };

        window.addEventListener('beforeunload', handleBeforeUnload);
        return () => window.removeEventListener('beforeunload', handleBeforeUnload);
    }, [content, initialContent, canEditContents, isSaving, saveDocument]);

    return (
        <ContentWrapper>
            <EditorSection
                onFocus={() => setIsEditorFocused(true)}
                onBlur={(e) => {
                    // Only blur if we're actually leaving the editor section
                    if (!e.currentTarget.contains(e.relatedTarget as Node)) {
                        setIsEditorFocused(false);
                        handleBlur();
                    }
                }}
            >
                {canEditContents ? (
                    <StyledEditor
                        key={`editor-${documentUrn}-${editorVersion}`}
                        content={content}
                        onChange={setContent}
                        placeholder="Write about anything..."
                        hideBorder
                        doNotFocus
                        $hideToolbar={!isEditorFocused}
                        fixedBottomToolbar={isEditorFocused}
                        uploadFile={uploadFile}
                        {...uploadFileAnalyticsCallbacks}
                    />
                ) : (
                    <StyledEditor
                        key={`editor-readonly-${documentUrn}-${editorVersion}`}
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
