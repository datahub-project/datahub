import React, { useState, useEffect } from 'react';
import { message } from 'antd';
import styled from 'styled-components/macro';
import analytics, { EventType, EntityActionType } from '../../../../../analytics';
import { GenericEntityUpdate } from '../../../types';
import { useEntityData, useEntityUpdate, useMutationUrn, useRefetch } from '../../../EntityContext';
import { useUpdateDescriptionMutation } from '../../../../../../graphql/mutations.generated';
import { DiscardDescriptionModal } from './DiscardDescriptionModal';
import { EDITED_DESCRIPTIONS_CACHE_NAME } from '../../../utils';
import { DescriptionEditorToolbar } from './DescriptionEditorToolbar';
import { Editor } from './editor/Editor';
import SourceDescription from './SourceDesription';

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
    const { entityType, entityData } = useEntityData();
    const refetch = useRefetch();
    const updateEntity = useEntityUpdate<GenericEntityUpdate>();
    const [updateDescriptionMutation] = useUpdateDescriptionMutation();

    const localStorageDictionary = localStorage.getItem(EDITED_DESCRIPTIONS_CACHE_NAME);
    const editedDescriptions = (localStorageDictionary && JSON.parse(localStorageDictionary)) || {};
    const description = editedDescriptions.hasOwnProperty(mutationUrn)
        ? editedDescriptions[mutationUrn]
        : entityData?.editableProperties?.description || entityData?.properties?.description || '';

    const [updatedDescription, setUpdatedDescription] = useState(description);
    const [isDescriptionUpdated, setIsDescriptionUpdated] = useState(editedDescriptions.hasOwnProperty(mutationUrn));
    const [confirmCloseModalVisible, setConfirmCloseModalVisible] = useState(false);

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

    // Function to handle all changes in Editor
    const handleEditorChange = (editedDescription: string) => {
        setUpdatedDescription(editedDescription);
        if (editedDescription === description) {
            setIsDescriptionUpdated(false);
        } else {
            setIsDescriptionUpdated(true);
        }
    };

    // Handling the Discard Modal
    const handleConfirmClose = (showConfirm: boolean | undefined = true) => {
        if (showConfirm && isDescriptionUpdated) {
            setConfirmCloseModalVisible(true);
        } else if (onComplete) onComplete();
    };

    const handleCloseWithoutSaving = () => {
        delete editedDescriptions[mutationUrn];
        if (Object.keys(editedDescriptions).length === 0) {
            localStorage.removeItem(EDITED_DESCRIPTIONS_CACHE_NAME);
        } else {
            localStorage.setItem(EDITED_DESCRIPTIONS_CACHE_NAME, JSON.stringify(editedDescriptions));
        }
        if (onComplete) onComplete();
    };

    return entityData ? (
        <>
            <DescriptionEditorToolbar
                onSave={handleSave}
                onClose={handleConfirmClose}
                disableSave={!isDescriptionUpdated}
            />
            <EditorSourceWrapper>
                <EditorContainer>
                    <Editor content={updatedDescription} onChange={handleEditorChange} />
                </EditorContainer>
                <SourceDescription />
            </EditorSourceWrapper>
            {confirmCloseModalVisible && (
                <DiscardDescriptionModal
                    cancelModalVisible={confirmCloseModalVisible}
                    onDiscard={handleCloseWithoutSaving}
                    onCancel={() => setConfirmCloseModalVisible(false)}
                />
            )}
        </>
    ) : null;
};
