import { Modal, Text } from '@components';
import { message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { ModalButton } from '@components/components/Modal/Modal';

import { useUserContext } from '@app/context/useUserContext';
import { ActorsSearchSelect } from '@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect';
import { createOwnerInputs } from '@app/entityV2/shared/utils/selectorUtils';
import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import TagDetailsSection from '@app/tags/CreateNewTagModal/TagDetailsSection';

import { useSetTagColorMutation } from '@graphql/mutations.generated';
import { useCreateTagMutation } from '@graphql/tag.generated';

const OwnersContainer = styled.div`
    margin-top: 24px;
    margin-bottom: 16px;
`;

const SectionLabel = styled(Text)`
    display: block;
    margin-bottom: 8px;
    font-weight: 600;
`;

type CreateNewTagModalProps = {
    open: boolean;
    onClose: () => void;
};

/**
 * Modal for creating a new tag with owners
 */
const CreateNewTagModal: React.FC<CreateNewTagModalProps> = ({ onClose, open }) => {
    // Tag details state
    const [tagName, setTagName] = useState('');
    const [tagDescription, setTagDescription] = useState('');
    const [tagColor, setTagColor] = useState('#1890ff');

    // Owners state
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const { user } = useUserContext();

    // Loading state
    const [isLoading, setIsLoading] = useState(false);

    // Mutations
    const [createTagMutation] = useCreateTagMutation();
    const [setTagColorMutation] = useSetTagColorMutation();

    /**
     * Handler for creating the tag with owners
     */
    const onOk = async () => {
        if (!tagName) {
            message.error('Tag name is required');
            return;
        }

        setIsLoading(true);

        try {
            // Convert selected owner URNs to OwnerInput format
            const ownerInputs = createOwnerInputs(selectedOwnerUrns);

            // Step 1: Create the new tag with owners
            const createTagResult = await createTagMutation({
                variables: {
                    input: {
                        id: tagName.trim(),
                        name: tagName.trim(),
                        description: tagDescription,
                        owners: ownerInputs.length > 0 ? ownerInputs : undefined,
                    },
                },
            });

            const newTagUrn = createTagResult.data?.createTag;

            if (!newTagUrn) {
                message.error('Failed to create tag. An unexpected error occurred');
                setIsLoading(false);
                return;
            }

            // Step 2: Add color
            if (tagColor) {
                await setTagColorMutation({
                    variables: {
                        urn: newTagUrn,
                        colorHex: tagColor,
                    },
                });
            }

            message.success(`Tag "${tagName}" successfully created`);
            onClose();
            setTagName('');
            setTagDescription('');
            setTagColor('#1890ff');
            setSelectedOwnerUrns([]);
        } catch (e: any) {
            message.destroy();
            message.error('Failed to create tag. An unexpected error occurred');
        } finally {
            setIsLoading(false);
        }
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createNewTagButton',
    });

    // Modal buttons configuration
    const buttons: ModalButton[] = [
        {
            text: 'Cancel',
            color: 'violet',
            variant: 'text',
            onClick: onClose,
            buttonDataTestId: 'create-tag-modal-cancel-button',
        },
        {
            text: 'Create',
            id: 'createNewTagButton',
            color: 'violet',
            variant: 'filled',
            onClick: onOk,
            disabled: !tagName || isLoading,
            isLoading,
            buttonDataTestId: 'create-tag-modal-create-button',
        },
    ];

    return (
        <Modal title="Create New Tag" onCancel={onClose} buttons={buttons} open={open} centered width={500}>
            {/* Tag Details Section */}
            <TagDetailsSection
                tagName={tagName}
                setTagName={setTagName}
                tagDescription={tagDescription}
                setTagDescription={setTagDescription}
                tagColor={tagColor}
                setTagColor={setTagColor}
            />

            {/* Owners Section */}
            <OwnersContainer>
                <SectionLabel>Add Owners</SectionLabel>
                <ActorsSearchSelect
                    selectedActorUrns={selectedOwnerUrns}
                    onUpdate={(actors) => setSelectedOwnerUrns(actors.map((actor) => actor.urn))}
                    placeholder="Search for users or groups"
                    defaultActors={user ? [user] : []}
                    width="full"
                />
            </OwnersContainer>
        </Modal>
    );
};

export default CreateNewTagModal;
