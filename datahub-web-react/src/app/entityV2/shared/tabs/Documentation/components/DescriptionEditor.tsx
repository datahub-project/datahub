import React, { useEffect, useRef, useState } from 'react';
import { message } from 'antd';
import styled from 'styled-components/macro';
import { useUpdateDescriptionMutation } from '../../../../../../graphql/mutations.generated';
import { useProposeUpdateDescriptionMutation } from '../../../../../../graphql/proposals.generated';
import { EntityType } from '../../../../../../types.generated';
import analytics, { EntityActionType, EventType } from '../../../../../analytics';
import { useEntityData, useEntityUpdate, useMutationUrn, useRefetch } from '../../../../../entity/shared/EntityContext';
import { GenericEntityUpdate } from '../../../../../entity/shared/types';
import { EDITED_DESCRIPTIONS_CACHE_NAME } from '../../../utils';
import { DescriptionEditorToolbar } from './DescriptionEditorToolbar';
import { Editor } from './editor/Editor';
import SourceDescription from './SourceDescription';
import { sanitizeRichText } from './editor/utils';
import { DiscardDescriptionModal } from './DiscardDescriptionModal';
import InferDocsPanel from '../../../components/inferredDocs/InferDocsPanel';
import { getAssetDescriptionDetails } from '../utils';
import { useIsDocumentationInferenceEnabled } from '../../../components/inferredDocs/utils';

const PROPOSAL_ENTITY_TYPES = [EntityType.GlossaryTerm, EntityType.GlossaryNode, EntityType.Dataset];

export function getShouldShowProposeButton(entityType: EntityType) {
    return PROPOSAL_ENTITY_TYPES.includes(entityType);
}

const EditorContainer = styled.div`
    flex: 1;
`;

const EditorSourceWrapper = styled.div`
    overflow: auto;
    display: flex;
    flex-direction: column;
    flex: 1;
`;

const InferDocsPanelWrapper = styled.div`
    padding: 16px;
`;

type DescriptionEditorProps = {
    inferOnMount?: boolean;
    onComplete?: () => void;
};

export const DescriptionEditor = ({ inferOnMount, onComplete }: DescriptionEditorProps) => {
    const mutationUrn = useMutationUrn();
    const { entityType, entityData, loading } = useEntityData();
    const refetch = useRefetch();
    const updateEntity = useEntityUpdate<GenericEntityUpdate>();
    const [updateDescriptionMutation] = useUpdateDescriptionMutation();
    const [proposeUpdateDescription] = useProposeUpdateDescriptionMutation();

    const localStorageDictionary = localStorage.getItem(EDITED_DESCRIPTIONS_CACHE_NAME);

    let { displayedDescription, isUsingDocumentationAspect } = getAssetDescriptionDetails({
        entityProperties: entityData,
        enableInferredDescriptions: useIsDocumentationInferenceEnabled(),
        defaultDescription: '',
    });

    const editedDescriptions = (localStorageDictionary && JSON.parse(localStorageDictionary)) || {};
    const shouldUseEditedDescription = editedDescriptions.hasOwnProperty(mutationUrn);
    const description = shouldUseEditedDescription ? editedDescriptions[mutationUrn] : displayedDescription;
    isUsingDocumentationAspect = !shouldUseEditedDescription && isUsingDocumentationAspect;

    const [updatedDescription, setUpdatedDescription] = useState(description);
    // Key to force re-render of the editor when the description is updated from server data. Only needed for full page refreshes mid edit
    const [editorKey, setEditorKey] = useState(0);
    const [isDescriptionUpdated, setIsDescriptionUpdated] = useState(
        editedDescriptions.hasOwnProperty(mutationUrn) || isUsingDocumentationAspect,
    );
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

    /**
     * If the documentation editor is refreshed mid edit, then this component will load without a description. if that description
     * comes in on the next frame, we need to update updatedDescription
     */
    const hasInitializedDescriptionRef = useRef(!updatedDescription);
    useEffect(() => {
        if (description && !updatedDescription && !hasInitializedDescriptionRef.current) {
            setUpdatedDescription(description);
            setEditorKey((prevKey) => prevKey + 1);
            hasInitializedDescriptionRef.current = true;
        } else if (updatedDescription) {
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

    function proposeUpdate() {
        const sanitizedDescription = sanitizeRichText(updatedDescription);
        proposeUpdateDescription({
            variables: {
                input: {
                    description: sanitizedDescription,
                    resourceUrn: mutationUrn,
                },
            },
        })
            .then(() => {
                message.success({ content: `Proposed description update!`, duration: 2 });
                setIsDescriptionUpdated(false);
                const editedDescriptionsLocal = (localStorageDictionary && JSON.parse(localStorageDictionary)) || {};
                delete editedDescriptionsLocal[mutationUrn];
                localStorage.setItem(EDITED_DESCRIPTIONS_CACHE_NAME, JSON.stringify(editedDescriptionsLocal));
                if (onComplete) onComplete();
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to propose: \n ${e.message || ''}`, duration: 3 });
            });
    }

    function handleCancel() {
        const editedDescriptionsLocal = (localStorageDictionary && JSON.parse(localStorageDictionary)) || {};
        delete editedDescriptionsLocal[mutationUrn];
        localStorage.setItem(EDITED_DESCRIPTIONS_CACHE_NAME, JSON.stringify(editedDescriptionsLocal));
        if (onComplete) onComplete();
    }

    // Function to handle all changes in Editor
    const handleEditorChange = (editedDescription: string) => {
        setUpdatedDescription(editedDescription);
        if (editedDescription === description) {
            setIsDescriptionUpdated(false);
        } else {
            setIsDescriptionUpdated(true);
        }
    };

    const handleCloseWithoutSaving = () => {
        delete editedDescriptions[mutationUrn];
        if (Object.keys(editedDescriptions).length === 0) {
            localStorage.removeItem(EDITED_DESCRIPTIONS_CACHE_NAME);
        } else {
            localStorage.setItem(EDITED_DESCRIPTIONS_CACHE_NAME, JSON.stringify(editedDescriptions || description));
        }
        if (onComplete) onComplete();
    };

    const shouldShowProposeButton = getShouldShowProposeButton(entityType);

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
                    <InferDocsPanelWrapper>
                        <InferDocsPanel
                            inferOnMount={inferOnMount}
                            onInsertDescription={(desc) => {
                                handleEditorChange(updatedDescription + desc);
                                setEditorKey((v) => v + 1);
                            }}
                        />
                    </InferDocsPanelWrapper>
                </EditorContainer>
                <SourceDescription />
            </EditorSourceWrapper>
            <DescriptionEditorToolbar
                onSave={handleSave}
                onPropose={proposeUpdate}
                onCancel={handleCancel}
                disableSave={!isDescriptionUpdated}
                showPropose={shouldShowProposeButton}
            />
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
