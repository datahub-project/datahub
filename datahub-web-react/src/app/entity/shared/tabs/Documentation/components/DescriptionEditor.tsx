import { message } from 'antd';
import DOMPurify from 'dompurify';
import queryString from 'query-string';
import React, { useCallback, useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components/macro';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityData, useEntityUpdate, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { DescriptionEditorToolbar } from '@app/entity/shared/tabs/Documentation/components/DescriptionEditorToolbar';
import { DiscardDescriptionModal } from '@app/entity/shared/tabs/Documentation/components/DiscardDescriptionModal';
import SourceDescription from '@app/entity/shared/tabs/Documentation/components/SourceDesription';
import { Editor } from '@app/entity/shared/tabs/Documentation/components/editor/Editor';
import { GenericEntityUpdate } from '@app/entity/shared/types';
import { EDITED_DESCRIPTIONS_CACHE_NAME } from '@app/entity/shared/utils';
import { useShouldShowInferDocumentationButton } from '@src/app/entityV2/shared/components/inferredDocs/utils';

import { useInferDocumentationMutation, useUpdateDescriptionMutation } from '@graphql/mutations.generated';
import { useProposeUpdateDescriptionMutation } from '@graphql/proposals.generated';
import { ActionRequestType, EntityType } from '@types';

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

type DescriptionEditorProps = {
    onComplete?: () => void;
};

export const DescriptionEditor = ({ onComplete }: DescriptionEditorProps) => {
    const mutationUrn = useMutationUrn();
    const { entityType, entityData } = useEntityData();
    const refetch = useRefetch();
    const updateEntity = useEntityUpdate<GenericEntityUpdate>();
    const [updateDescriptionMutation] = useUpdateDescriptionMutation();
    const [proposeUpdateDescription] = useProposeUpdateDescriptionMutation();
    const [inferDocumentation] = useInferDocumentationMutation();

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

    function proposeUpdate() {
        const sanitizedDescription = DOMPurify.sanitize(updatedDescription);
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
                analytics.event({
                    type: EventType.EntityActionEvent,
                    actionType: EntityActionType.ProposalCreated,
                    actionQualifier: ActionRequestType.UpdateDescription,
                    entityType,
                    entityUrn: mutationUrn,
                });
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

    const handleGenerate = useCallback(() => {
        message.loading({ content: 'Generating...' });
        inferDocumentation({
            variables: {
                urn: mutationUrn,
                saveResult: false,
            },
        })
            .then(({ data, errors }) => {
                if (errors) {
                    // Errors are handled in the catch block.
                    throw new Error(errors[0].message);
                }
                message.destroy();

                const suggestedDescription = data?.inferDocumentation?.entityDescription || '';
                setUpdatedDescription((prevDescription) => prevDescription + suggestedDescription);
                setIsDescriptionUpdated(true);
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to generate: \n ${e.message || ''}`, duration: 3 });
            });
    }, [mutationUrn, inferDocumentation, setUpdatedDescription, setIsDescriptionUpdated]);

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

    const shouldShowProposeButton = getShouldShowProposeButton(entityType);
    const shouldShowGenerateButton = useShouldShowInferDocumentationButton(entityType);

    // If the generate param is set in the URL, run suggestion generation on load.
    const location = useLocation();
    const history = useHistory();
    const needsGenerate = queryString.parse(location.search, { parseBooleans: true }).generate;

    useEffect(() => {
        if (shouldShowGenerateButton && needsGenerate) {
            handleGenerate();

            // Remove the generate param from the URL.
            history.replace({
                pathname: location.pathname,
                search: queryString.stringify({
                    ...queryString.parse(location.search, { parseBooleans: true }),
                    generate: undefined,
                }),
            });
        }
    }, [shouldShowGenerateButton, needsGenerate, history, location, handleGenerate]);

    return entityData ? (
        <>
            <DescriptionEditorToolbar
                onSave={handleSave}
                onPropose={proposeUpdate}
                onClose={handleConfirmClose}
                onGenerate={handleGenerate}
                disableSave={!isDescriptionUpdated}
                showPropose={shouldShowProposeButton}
                showGenerate={shouldShowGenerateButton}
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
