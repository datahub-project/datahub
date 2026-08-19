import { Editor } from '@components';
import { Modal, message } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityData, useEntityUpdate, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { GenericEntityUpdate } from '@app/entity/shared/types';
import { DescriptionEditorToolbar } from '@app/entityV2/shared/tabs/Documentation/components/DescriptionEditorToolbar';
import SourceDescription from '@app/entityV2/shared/tabs/Documentation/components/SourceDescription';
import { getAssetDescriptionDetails } from '@app/entityV2/shared/tabs/Documentation/utils';
import { EDITED_DESCRIPTIONS_CACHE_NAME } from '@app/entityV2/shared/utils';
import useFileUpload from '@app/shared/hooks/useFileUpload';
import useFileUploadAnalyticsCallbacks from '@app/shared/hooks/useFileUploadAnalyticsCallbacks';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { UploadDownloadScenario } from '@types';

const EditorContainer = styled.div`
    flex: 1;
`;

const EditorSourceWrapper = styled.div`
    overflow: auto;
    display: flex;
    flex-direction: column;
    flex: 1;
`;

type DescriptionEditorProps = {
    onComplete?: () => void;
};

export const DescriptionEditor = ({ onComplete }: DescriptionEditorProps) => {
    const { t } = useTranslation('entity.profile.documentation');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const mutationUrn = useMutationUrn();
    const { entityType, entityData, loading } = useEntityData();
    const refetch = useRefetch();

    const uploadFileAnalyticsCallbacks = useFileUploadAnalyticsCallbacks({
        scenario: UploadDownloadScenario.AssetDocumentation,
        assetUrn: mutationUrn,
    });

    const { uploadFile } = useFileUpload({
        scenario: UploadDownloadScenario.AssetDocumentation,
        assetUrn: mutationUrn,
    });

    const updateEntity = useEntityUpdate<GenericEntityUpdate>();
    const [updateDescriptionMutation] = useUpdateDescriptionMutation();

    const localStorageDictionary = localStorage.getItem(EDITED_DESCRIPTIONS_CACHE_NAME);

    const { displayedDescription, isUsingDocumentationAspect: isUsingDocumentationAspectRaw } =
        getAssetDescriptionDetails({
            entityProperties: entityData,
            defaultDescription: '',
        });

    const editedDescriptions = (localStorageDictionary && JSON.parse(localStorageDictionary)) || {};
    const shouldUseEditedDescription = editedDescriptions.hasOwnProperty(mutationUrn);
    const description = shouldUseEditedDescription ? editedDescriptions[mutationUrn] : displayedDescription;
    const isUsingDocumentationAspect = !shouldUseEditedDescription && isUsingDocumentationAspectRaw;

    const [updatedDescription, setUpdatedDescription] = useState(description);
    // Key to force re-render of the editor when the description is updated from server data. Only needed for full page refreshes mid edit
    const [editorKey, setEditorKey] = useState(0);
    const [isDescriptionUpdated, setIsDescriptionUpdated] = useState(
        editedDescriptions.hasOwnProperty(mutationUrn) || isUsingDocumentationAspect,
    );
    const hasUnsavedChangesRef = useRef(isDescriptionUpdated);
    hasUnsavedChangesRef.current = isDescriptionUpdated;

    /**
     * Auto-Save the description edits to local storage every 5 seconds.
     */
    useEffect(() => {
        let delayDebounceFn: ReturnType<typeof setTimeout>;
        const editedDescriptionsLocal = (localStorageDictionary && JSON.parse(localStorageDictionary)) || {};

        if (isDescriptionUpdated) {
            delayDebounceFn = setTimeout(() => {
                editedDescriptionsLocal[mutationUrn] = updatedDescription;
                localStorage.setItem(EDITED_DESCRIPTIONS_CACHE_NAME, JSON.stringify(editedDescriptionsLocal));
            }, 5000);
        }
        return () => clearTimeout(delayDebounceFn);
    }, [mutationUrn, isDescriptionUpdated, updatedDescription, localStorageDictionary]);

    /**
     * If the documentation editor is refreshed mid edit, then this component will load without a description. if that description
     * comes in on the next frame, we need to update updatedDescription
     */
    const hasInitializedDescriptionRef = useRef(!!updatedDescription);
    useEffect(() => {
        if (hasInitializedDescriptionRef.current) return;
        if (updatedDescription) {
            hasInitializedDescriptionRef.current = true;
        } else if (description) {
            setUpdatedDescription(description);
            setEditorKey((prevKey) => prevKey + 1);
            hasInitializedDescriptionRef.current = true;
        }
    }, [description, updatedDescription]);

    const updateDescriptionLegacy = () => {
        return updateEntity?.({
            variables: { urn: mutationUrn, input: { editableProperties: { description: updatedDescription || '' } } },
        });
    };

    const updateDescription = () => {
        return updateDescriptionMutation({
            variables: {
                input: {
                    description: updatedDescription,
                    resourceUrn: mutationUrn,
                },
            },
        });
    };

    const handleSave = async () => {
        message.loading({ content: tf('saving') });
        try {
            if (updateEntity) {
                // Use the legacy update description path.
                await updateDescriptionLegacy();
            } else {
                // Use the new update description path.
                await updateDescription();
            }
            message.destroy();
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: EntityActionType.UpdateDescription,
                entityType,
                entityUrn: mutationUrn,
            });
            message.success({ content: t('descriptionUpdated'), duration: 2 });
            // Updating the localStorage after save
            delete editedDescriptions[mutationUrn];
            if (Object.keys(editedDescriptions).length === 0) {
                localStorage.removeItem(EDITED_DESCRIPTIONS_CACHE_NAME);
            } else {
                localStorage.setItem(EDITED_DESCRIPTIONS_CACHE_NAME, JSON.stringify(editedDescriptions));
            }
            if (onComplete) onComplete();
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: t('failedToUpdateDescription', { message: e.message || '' }), duration: 2 });
            }
        }
        refetch?.();
    };

    function handleCancel() {
        const onCancel = () => {
            delete editedDescriptions[mutationUrn];
            if (Object.keys(editedDescriptions).length === 0) {
                localStorage.removeItem(EDITED_DESCRIPTIONS_CACHE_NAME);
            } else {
                localStorage.setItem(EDITED_DESCRIPTIONS_CACHE_NAME, JSON.stringify(editedDescriptions || description));
            }
            if (onComplete) onComplete();
        };
        if (!hasUnsavedChangesRef.current) {
            onCancel();
        } else {
            Modal.confirm({
                title: t('discardChanges.title'),
                content: t('discardChanges.description'),
                onOk: onCancel,
                onCancel() {},
                okText: tc('yes'),
                maskClosable: true,
                closable: true,
            });
        }
    }

    // Prevent closing tab by mistake
    useEffect(() => {
        function onBeforeUnload(e) {
            if (hasUnsavedChangesRef.current) {
                // This automatically shows the unsaved changes alert.
                e.preventDefault();
                e.returnValue = '';
            }
        }

        window.addEventListener('beforeunload', onBeforeUnload);

        return () => window.removeEventListener('beforeunload', onBeforeUnload);
    }, []);

    // Function to handle all changes in Editor
    const handleEditorChange = (editedDescription: string) => {
        setUpdatedDescription(editedDescription);
        if (editedDescription === description) {
            setIsDescriptionUpdated(false);
        } else {
            setIsDescriptionUpdated(true);
        }
    };

    return !loading ? (
        <>
            <EditorSourceWrapper>
                <EditorContainer data-testid="description-editor">
                    <Editor
                        key={editorKey}
                        content={updatedDescription}
                        onChange={handleEditorChange}
                        placeholder={t('editorPlaceholder')}
                        uploadFileProps={{
                            onFileUpload: uploadFile,
                            ...uploadFileAnalyticsCallbacks,
                        }}
                        hideBorder
                    />
                </EditorContainer>
                <SourceDescription />
            </EditorSourceWrapper>
            <DescriptionEditorToolbar onSave={handleSave} onCancel={handleCancel} disableSave={!isDescriptionUpdated} />
        </>
    ) : null;
};
