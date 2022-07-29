import React, { useState, useEffect } from 'react';
import { message, Button } from 'antd';
import { CheckOutlined, MailOutlined } from '@ant-design/icons';
import DOMPurify from 'dompurify';
import styled from 'styled-components/macro';

import analytics, { EventType, EntityActionType } from '../../../../../analytics';

import StyledMDEditor from '../../../components/styled/StyledMDEditor';
import TabToolbar from '../../../components/styled/TabToolbar';

import { GenericEntityUpdate } from '../../../types';
import { useEntityData, useEntityUpdate, useMutationUrn, useRefetch } from '../../../EntityContext';
import { useUpdateDescriptionMutation } from '../../../../../../graphql/mutations.generated';
import { DiscardDescriptionModal } from './DiscardDescriptionModal';
import { EDITED_DESCRIPTIONS_CACHE_NAME } from '../../../utils';
import { useProposeUpdateDescriptionMutation } from '../../../../../../graphql/proposals.generated';
import { EntityType } from '../../../../../../types.generated';

const ProposeButton = styled(Button)`
    margin-right: 10px;
`;

export function getShouldShowProposeButton(entityType: EntityType) {
    return entityType === EntityType.GlossaryTerm || entityType === EntityType.GlossaryNode;
}

export const DescriptionEditor = ({ onComplete }: { onComplete?: () => void }) => {
    const mutationUrn = useMutationUrn();
    const { entityType, entityData } = useEntityData();
    const refetch = useRefetch();
    const updateEntity = useEntityUpdate<GenericEntityUpdate>();
    const [updateDescriptionMutation] = useUpdateDescriptionMutation();
    const [proposeUpdateDescription] = useProposeUpdateDescriptionMutation();

    const localStorageDictionary = localStorage.getItem(EDITED_DESCRIPTIONS_CACHE_NAME);
    const editedDescriptions = (localStorageDictionary && JSON.parse(localStorageDictionary)) || {};
    const description = editedDescriptions.hasOwnProperty(mutationUrn)
        ? editedDescriptions[mutationUrn]
        : entityData?.editableProperties?.description || entityData?.properties?.description || '';

    const [updatedDescription, setUpdatedDescription] = useState(description);
    const [isDescriptionUpdated, setIsDescriptionUpdated] = useState(editedDescriptions.hasOwnProperty(mutationUrn));
    const [cancelModalVisible, setCancelModalVisible] = useState(false);

    const updateDescriptionLegacy = () => {
        const sanitizedDescription = DOMPurify.sanitize(updatedDescription);
        return updateEntity?.({
            variables: { urn: mutationUrn, input: { editableProperties: { description: sanitizedDescription || '' } } },
        });
    };

    const updateDescription = () => {
        const sanitizedDescription = DOMPurify.sanitize(updatedDescription);
        return updateDescriptionMutation({
            variables: {
                input: {
                    description: sanitizedDescription,
                    resourceUrn: mutationUrn,
                },
            },
        });
    };

    const handleSaveDescription = async () => {
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
                setIsDescriptionUpdated(false);
                const editedDescriptionsLocal = (localStorageDictionary && JSON.parse(localStorageDictionary)) || {};
                delete editedDescriptionsLocal[mutationUrn];
                localStorage.setItem(EDITED_DESCRIPTIONS_CACHE_NAME, JSON.stringify(editedDescriptionsLocal));
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to propose: \n ${e.message || ''}`, duration: 3 });
            });
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

    // Updating the localStorage when the user has paused for 5 sec
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

    // Handling the Discard Modal
    const showModal = () => {
        if (isDescriptionUpdated) {
            setCancelModalVisible(true);
        } else if (onComplete) onComplete();
    };

    function onCancel() {
        setCancelModalVisible(false);
    }

    const onDiscard = () => {
        delete editedDescriptions[mutationUrn];
        if (Object.keys(editedDescriptions).length === 0) {
            localStorage.removeItem(EDITED_DESCRIPTIONS_CACHE_NAME);
        } else {
            localStorage.setItem(EDITED_DESCRIPTIONS_CACHE_NAME, JSON.stringify(editedDescriptions));
        }
        if (onComplete) onComplete();
    };

    const shouldShowProposeButton = getShouldShowProposeButton(entityType);

    return entityData ? (
        <>
            <TabToolbar>
                <Button type="text" onClick={showModal}>
                    Back
                </Button>
                <div>
                    {shouldShowProposeButton && (
                        <ProposeButton onClick={proposeUpdate} disabled={!isDescriptionUpdated}>
                            <MailOutlined /> Propose
                        </ProposeButton>
                    )}
                    <Button onClick={handleSaveDescription} disabled={!isDescriptionUpdated}>
                        <CheckOutlined /> Save
                    </Button>
                </div>
            </TabToolbar>
            <StyledMDEditor
                value={updatedDescription}
                onChange={(v) => handleEditorChange(v || '')}
                preview="live"
                visiableDragbar={false}
            />
            {cancelModalVisible && (
                <DiscardDescriptionModal
                    cancelModalVisible={cancelModalVisible}
                    onDiscard={onDiscard}
                    onCancel={onCancel}
                />
            )}
        </>
    ) : null;
};
