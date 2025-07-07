import { Modal } from '@components';
import { message } from 'antd';
import React, { useState } from 'react';

import { ModalButton } from '@components/components/Modal/Modal';

import { useEnterKeyListener } from '@app/shared/useEnterKeyListener';
import OwnersSection, { PendingOwner } from '@app/sharedV2/owners/OwnersSection';
import TagDetailsSection from '@app/tags/CreateNewTagModal/TagDetailsSection';

import { useBatchAddOwnersMutation, useSetTagColorMutation } from '@graphql/mutations.generated';
import { useCreateTagMutation } from '@graphql/tag.generated';

type CreateNewTagModalProps = {
    open: boolean;
    onClose: () => void;
};

/**
 * Modal for creating a new tag with owners and applying it to entities
 */
const CreateNewTagModal: React.FC<CreateNewTagModalProps> = ({ onClose, open }) => {
    // Tag details state
    const [tagName, setTagName] = useState('');
    const [tagDescription, setTagDescription] = useState('');
    const [tagColor, setTagColor] = useState('#1890ff');

    // Owners state
    const [pendingOwners, setPendingOwners] = useState<PendingOwner[]>([]);
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);

    // Loading state
    const [isLoading, setIsLoading] = useState(false);

    // Mutations
    const [createTagMutation] = useCreateTagMutation();
    const [setTagColorMutation] = useSetTagColorMutation();
    const [batchAddOwnersMutation] = useBatchAddOwnersMutation();

    const onChangeOwners = (newOwners: PendingOwner[]) => {
        setPendingOwners(newOwners);
    };

    /**
     * Handler for creating the tag and applying it to entities
     */
    const onOk = async () => {
        if (!tagName) {
            message.error('Tag name is required');
            return;
        }

        setIsLoading(true);

        try {
            // Step 1: Create the new tag
            const createTagResult = await createTagMutation({
                variables: {
                    input: {
                        id: tagName.trim(),
                        name: tagName.trim(),
                        description: tagDescription,
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

            // Step 3: Add owners if any
            if (pendingOwners.length > 0) {
                await batchAddOwnersMutation({
                    variables: {
                        input: {
                            owners: pendingOwners,
                            resources: [{ resourceUrn: newTagUrn }],
                        },
                    },
                });
            }

            message.success(`Tag "${tagName}" successfully created`);
            onClose();
            setTagName('');
            setTagDescription('');
            setTagColor('#1890ff');
            setPendingOwners([]);
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
            <OwnersSection
                selectedOwnerUrns={selectedOwnerUrns}
                setSelectedOwnerUrns={setSelectedOwnerUrns}
                existingOwners={[]}
                onChange={onChangeOwners}
            />
        </Modal>
    );
};

export default CreateNewTagModal;
