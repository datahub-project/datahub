import { Editor } from '@components';
import { Modal, message } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components/macro';

import { sanitizeRichText } from '@components/components/Editor/utils';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityData, useEntityUpdate, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { GenericEntityUpdate } from '@app/entity/shared/types';
import InferDocsPanel from '@app/entityV2/shared/components/inferredDocs/InferDocsPanel';
import {
    useIsDocumentationInferenceEnabled,
    useShouldShowInferDocumentationButton,
} from '@app/entityV2/shared/components/inferredDocs/utils';
import ProposalDescriptionModal from '@app/entityV2/shared/containers/profile/sidebar/ProposalDescriptionModal';
import { DescriptionEditorToolbar } from '@app/entityV2/shared/tabs/Documentation/components/DescriptionEditorToolbar';
import SourceDescription from '@app/entityV2/shared/tabs/Documentation/components/SourceDescription';
import { getAssetDescriptionDetails } from '@app/entityV2/shared/tabs/Documentation/utils';
import { EDITED_DESCRIPTIONS_CACHE_NAME } from '@app/entityV2/shared/utils';
import { useAppConfig } from '@src/app/useAppConfig';

import { useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { useProposeUpdateDescriptionMutation } from '@graphql/proposals.generated';
import { EntityType } from '@types';

export const PROPOSAL_ENTITY_TYPES = [
    EntityType.GlossaryTerm,
    EntityType.GlossaryNode,
    EntityType.Dataset,
    EntityType.Container,
    EntityType.Chart,
    EntityType.Dashboard,
    EntityType.Domain,
    EntityType.Mlfeature,
    EntityType.MlfeatureTable,
    EntityType.Mlmodel,
    EntityType.MlmodelGroup,
    EntityType.MlprimaryKey,
    EntityType.DataFlow,
    EntityType.DataJob,
    EntityType.DataProduct,
];

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

    const canEditDescription = entityData?.privileges?.canEditDescription;
    const canProposeDescription = entityData?.privileges?.canProposeDescription;

    const shouldShowInferDocsAction = useShouldShowInferDocumentationButton(entityType);

    const updateEntity = useEntityUpdate<GenericEntityUpdate>();
    const [updateDescriptionMutation] = useUpdateDescriptionMutation();
    const [proposeUpdateDescription] = useProposeUpdateDescriptionMutation();

    const localStorageDictionary = localStorage.getItem(EDITED_DESCRIPTIONS_CACHE_NAME);

    const { displayedDescription, isUsingDocumentationAspect: isUsingDocumentationAspectRaw } =
        getAssetDescriptionDetails({
            entityProperties: entityData,
            enableInferredDescriptions: useIsDocumentationInferenceEnabled(),
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

    const [showProposeModal, setShowProposeModal] = useState(false);
    const { config } = useAppConfig();
    const { showTaskCenterRedesign } = config.featureFlags;

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

    function proposeUpdate(proposalNote?: string) {
        const sanitizedDescription = sanitizeRichText(updatedDescription);
        proposeUpdateDescription({
            variables: {
                input: {
                    description: sanitizedDescription,
                    resourceUrn: mutationUrn,
                    proposalNote,
                },
            },
        })
            .then(() => {
                message.success({ content: `Proposed description update!`, duration: 2 });
                setIsDescriptionUpdated(false);
                const editedDescriptionsLocal = (localStorageDictionary && JSON.parse(localStorageDictionary)) || {};
                delete editedDescriptionsLocal[mutationUrn];
                localStorage.setItem(EDITED_DESCRIPTIONS_CACHE_NAME, JSON.stringify(editedDescriptionsLocal));
                setShowProposeModal(false);
                if (onComplete) onComplete();
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to propose: \n ${e.message || ''}`, duration: 3 });
            });
    }

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

    const shouldShowProposeButton = getShouldShowProposeButton(entityType);

    const handlePropose = () => {
        if (showTaskCenterRedesign) {
            setShowProposeModal(true);
        } else {
            proposeUpdate();
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
                        hideBorder
                    />
                    {shouldShowInferDocsAction && (
                        <InferDocsPanelWrapper>
                            <InferDocsPanel
                                urn={mutationUrn}
                                inferOnMount={inferOnMount}
                                onInsertDescription={(desc) => {
                                    handleEditorChange(updatedDescription + desc);
                                    setEditorKey((v) => v + 1);
                                }}
                                surface="entity-docs-editor"
                            />
                        </InferDocsPanelWrapper>
                    )}
                </EditorContainer>
                <SourceDescription />
            </EditorSourceWrapper>
            <DescriptionEditorToolbar
                onSave={handleSave}
                onPropose={handlePropose}
                onCancel={handleCancel}
                disableSave={!isDescriptionUpdated || !canEditDescription}
                disablePropose={!shouldShowProposeButton || !isDescriptionUpdated || !canProposeDescription}
            />
            {showProposeModal && (
                <ProposalDescriptionModal onPropose={proposeUpdate} onCancel={() => setShowProposeModal(false)} />
            )}
        </>
    ) : null;
};
