import { Editor } from '@components';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import useClickOutside from '@components/components/Utils/ClickOutside/useClickOutside';

import { useContextLayout } from '@app/context/ContextLayoutContext';
import { useDocumentPermissions } from '@app/document/hooks/useDocumentPermissions';
import { useExtractMentions } from '@app/document/hooks/useExtractMentions';
import { useUpdateDocument } from '@app/document/hooks/useUpdateDocument';
import { extractUrnsFromMarkdown, isAllowedRelatedAssetUrn } from '@app/document/utils/documentUtils';
import { useRefetch } from '@app/entity/shared/EntityContext';
import { RelatedSection } from '@app/entityV2/document/summary/RelatedSection';
import useFileUpload from '@app/shared/hooks/useFileUpload';
import useFileUploadAnalyticsCallbacks from '@app/shared/hooks/useFileUploadAnalyticsCallbacks';

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
            min-height: 460px;
        }
        .remirror-editor.ProseMirror {
            font-size: 15px;
            line-height: 1.7;
            color: ${(props) => props.theme.colors.text};
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
    const editorSectionRef = useRef<HTMLDivElement>(null);
    const { canEditContents } = useDocumentPermissions(documentUrn);
    const { updateContents, updateRelatedEntities } = useUpdateDocument();
    const refetch = useRefetch();
    // Extract mentions from content (currently unused, but hook needs to run)
    useExtractMentions(content);

    // Get layout context for toolbar positioning (only available in Context Documents layout)
    const contextLayout = useContextLayout();

    // Calculate toolbar styles to center on content area when sidebar is present
    const toolbarStyles = useMemo((): React.CSSProperties | undefined => {
        if (!contextLayout) return undefined;

        // Offset the toolbar center by half the sidebar width
        const offset = contextLayout.sidebarWidth / 2;
        return {
            left: `calc(50% + ${offset}px)`,
            transform: `translateX(-40%)`,
        };
    }, [contextLayout]);

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
                // Extract URNs from markdown links using balanced-parenthesis parser
                // This correctly handles nested URNs like dataJob:(dataFlow:(...),task)
                const extractedUrns = extractUrnsFromMarkdown(contentToSave);
                const documentUrnsToSave: string[] = [];
                const assetUrnsToSave: string[] = [];

                extractedUrns.forEach((urn) => {
                    // Check if it's a document URN
                    if (urn.includes(':document:')) {
                        if (!documentUrnsToSave.includes(urn)) {
                            documentUrnsToSave.push(urn);
                        }
                    } else if (isAllowedRelatedAssetUrn(urn)) {
                        // Only add to related assets if it passes validation
                        // (balanced parens, not a disallowed entity type like corpUser/corpGroup)
                        if (!assetUrnsToSave.includes(urn)) {
                            assetUrnsToSave.push(urn);
                        }
                    }
                });

                // Merge new URNs with existing ones (additive, not replacement)
                // Get existing URNs
                const existingAssetUrns = new Set(relatedAssets?.map((ra) => ra.asset.urn) || []);
                const existingDocumentUrns = new Set(relatedDocuments?.map((rd) => rd.document.urn) || []);

                // Add new URNs to existing sets (automatically handles duplicates)
                assetUrnsToSave.forEach((urn) => existingAssetUrns.add(urn));
                documentUrnsToSave.forEach((urn) => existingDocumentUrns.add(urn));

                // Convert back to arrays
                const finalAssetUrns = Array.from(existingAssetUrns);
                const finalDocumentUrns = Array.from(existingDocumentUrns);

                // Save content
                await updateContents({
                    urn: documentUrn,
                    contents: { text: contentToSave },
                });

                // Update related entities - merge new mentions with existing ones
                await updateRelatedEntities({
                    urn: documentUrn,
                    relatedAssets: finalAssetUrns,
                    relatedDocuments: finalDocumentUrns,
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
        [
            isSaving,
            initialContent,
            canEditContents,
            updateContents,
            updateRelatedEntities,
            documentUrn,
            refetch,
            relatedAssets,
            relatedDocuments,
        ],
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

    const handleClickOutside = useCallback(() => {
        setIsEditorFocused(false);
        handleBlur();
    }, [handleBlur]);

    const clickOutsideOptions = useMemo(
        () => ({
            wrappers: [editorSectionRef],
            ignoreSelector: '.ant-dropdown',
        }),
        [],
    );

    useClickOutside(handleClickOutside, clickOutsideOptions);

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

    // Handle updating related entities (supports both adding and removing)
    // The passed URNs represent the final desired list after user selections/deselections
    const handleAddEntities = useCallback(
        async (assetUrns: string[], documentUrns: string[]) => {
            // The URNs passed here are the final list (after user selections/deselections in the dropdown)
            // So we replace the entire list, which handles both additions and removals
            await updateRelatedEntities({
                urn: documentUrn,
                relatedAssets: assetUrns,
                relatedDocuments: documentUrns,
            });

            // Refetch to get updated data
            await refetch();
        },
        [documentUrn, updateRelatedEntities, refetch],
    );

    // Handle removing a single related entity
    const handleRemoveEntity = useCallback(
        async (urnToRemove: string) => {
            // Get existing URNs
            const existingAssetUrns = relatedAssets?.map((ra) => ra.asset.urn) || [];
            const existingDocumentUrns = relatedDocuments?.map((rd) => rd.document.urn) || [];

            // Remove the URN from the appropriate list
            const isDocument = urnToRemove.includes(':document:');
            const finalAssetUrns = isDocument
                ? existingAssetUrns
                : existingAssetUrns.filter((urn) => urn !== urnToRemove);
            const finalDocumentUrns = isDocument
                ? existingDocumentUrns.filter((urn) => urn !== urnToRemove)
                : existingDocumentUrns;

            await updateRelatedEntities({
                urn: documentUrn,
                relatedAssets: finalAssetUrns,
                relatedDocuments: finalDocumentUrns,
            });

            // Refetch to get updated data
            await refetch();
        },
        [documentUrn, updateRelatedEntities, refetch, relatedAssets, relatedDocuments],
    );

    return (
        <ContentWrapper>
            <EditorSection
                ref={editorSectionRef}
                data-testid="document-editor-section"
                onFocus={() => setIsEditorFocused(true)}
            >
                {canEditContents ? (
                    <StyledEditor
                        data-testid="document-content-editor"
                        key={`editor-${documentUrn}-${editorVersion}`}
                        content={content}
                        onChange={setContent}
                        placeholder="Write about anything..."
                        hideBorder
                        doNotFocus
                        $hideToolbar={!isEditorFocused}
                        fixedBottomToolbar={isEditorFocused}
                        toolbarStyles={toolbarStyles}
                        uploadFileProps={{
                            onFileUpload: uploadFile,
                            ...uploadFileAnalyticsCallbacks,
                        }}
                    />
                ) : (
                    <StyledEditor
                        data-testid="document-content-editor-readonly"
                        key={`editor-readonly-${documentUrn}-${editorVersion}`}
                        content={content}
                        readOnly
                        placeholder="No content"
                        hideBorder
                    />
                )}
            </EditorSection>

            {!isEditorFocused && (
                <RelatedSection
                    relatedAssets={relatedAssets}
                    relatedDocuments={relatedDocuments}
                    documentUrn={documentUrn}
                    onAddEntities={handleAddEntities}
                    onRemoveEntity={handleRemoveEntity}
                    canEdit={canEditContents}
                />
            )}
        </ContentWrapper>
    );
};
