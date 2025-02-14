import React, { useEffect, useRef, useState } from 'react';
import { message, Modal } from 'antd';
import styled from 'styled-components/macro';
import { useUpdateDescriptionMutation } from '../../../../../../graphql/mutations.generated';
import analytics, { EntityActionType, EventType } from '../../../../../analytics';
import { useEntityData, useEntityUpdate, useMutationUrn, useRefetch } from '../../../../../entity/shared/EntityContext';
import { GenericEntityUpdate } from '../../../../../entity/shared/types';
import { EDITED_DESCRIPTIONS_CACHE_NAME } from '../../../utils';
import { DescriptionEditorToolbar } from './DescriptionEditorToolbar';
import { Editor } from './editor/Editor';
import SourceDescription from './SourceDescription';
import { getAssetDescriptionDetails } from '../utils';

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
    const mutationUrn = useMutationUrn();
    const { entityType, entityData, loading } = useEntityData();
    const refetch = useRefetch();

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
        message.loading({ content: 'Saving...' });
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
            message.success({ content: 'Description Updated', duration: 2 });
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
                message.error({ content: `Failed to update description: \n ${e.message || ''}`, duration: 2 });
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
                title: `Discard unsaved changes?`,
                content: `Your changes will be lost.`,
                onOk: onCancel,
                onCancel() {},
                okText: 'Yes',
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
                <EditorContainer>
                    <Editor
                        key={editorKey}
                        content={updatedDescription}
                        onChange={handleEditorChange}
                        placeholder="Describe this asset to make it more discoverable. Tag @user or reference @asset to make your docs come to life!"
                    />
                </EditorContainer>
                <SourceDescription />
            </EditorSourceWrapper>
            <DescriptionEditorToolbar onSave={handleSave} onCancel={handleCancel} disableSave={!isDescriptionUpdated} />
        </>
    ) : null;
};
