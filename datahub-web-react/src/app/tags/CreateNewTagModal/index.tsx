import React, { useState } from 'react';
import { message } from 'antd';
import { Modal, Text } from '@components';
import { ResourceRefInput } from '../../../types.generated';
import { useCreateTagMutation } from '../../../graphql/tag.generated';
import { useEnterKeyListener } from '../../shared/useEnterKeyListener';
import { useUserContext } from '../../context/useUserContext';
import {
    useBatchAddOwnersMutation,
    useBatchAddTagsMutation,
    useSetTagColorMutation,
} from '../../../graphql/mutations.generated';
import { CreateNewTagModalProps, ModalButton, PendingOwner } from './types';
import TagDetailsSection from './TagDetailsSection';
import OwnersSection from './OwnersSection';
import EntitiesSection from './EntitiesSection';

/**
 * Modal for creating a new tag with owners and applying it to entities
 */
const CreateNewTagModal: React.FC<CreateNewTagModalProps> = ({ onClose, open }) => {
    // Tag details state
    const [tagName, setTagName] = useState('');
    const [tagDescription, setTagDescription] = useState('');
    const [tagColor, setTagColor] = useState('#1890ff');

    // Owners state
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const [pendingOwners, setPendingOwners] = useState<PendingOwner[]>([]);

    // Entity selection state
    const [selectedEntityUrns, setSelectedEntityUrns] = useState<string[]>([]);

    // Loading state
    const [isLoading, setIsLoading] = useState(false);

    // Mutations
    const [createTagMutation] = useCreateTagMutation();
    const [setTagColorMutation] = useSetTagColorMutation();
    const [batchAddOwnersMutation] = useBatchAddOwnersMutation();
    const [batchAddTagsMutation] = useBatchAddTagsMutation();

    // Check permissions using UserContext
    const userContext = useUserContext();
    const canCreateTags = userContext?.platformPrivileges?.createTags || userContext?.platformPrivileges?.manageTags;

    /**
     * Handler for creating the tag and applying it to entities
     */
    const onOk = async () => {
        if (!canCreateTags) {
            message.error('You do not have permission to create tags');
            return;
        }

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
                        id: tagName,
                        name: tagName,
                        description: tagDescription,
                    },
                },
            });

            const newTagUrn = createTagResult.data?.createTag;

            if (!newTagUrn) {
                throw new Error('Failed to create tag: No URN returned');
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

            // Step 4: Apply tag to selected entities
            if (selectedEntityUrns.length > 0) {
                const resources: ResourceRefInput[] = selectedEntityUrns.map((entityUrn) => ({
                    resourceUrn: entityUrn,
                }));

                await batchAddTagsMutation({
                    variables: {
                        input: {
                            tagUrns: [newTagUrn],
                            resources,
                        },
                    },
                });
            }

            message.success(
                `Tag "${tagName}" successfully created${
                    selectedEntityUrns.length > 0 ? ' and applied to selected entities' : ''
                }`,
            );
            onClose();
        } catch (e: any) {
            message.destroy();
            message.error({ content: `Failed to create tag: \n ${e.message || ''}`, duration: 3 });
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
        },
        {
            text: 'Create',
            id: 'createNewTagButton',
            color: 'violet',
            variant: 'filled',
            onClick: onOk,
            disabled: !tagName || isLoading || !canCreateTags,
            isLoading,
        },
    ];

    // Render permission denied modal if user can't create tags
    if (!canCreateTags) {
        return (
            <Modal title="Create New Tag" onCancel={onClose} buttons={buttons} open={open} centered width={400}>
                <p>You don&apos;t have permission to create tags. Please contact your DataHub administrator.</p>
            </Modal>
        );
    }

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
                pendingOwners={pendingOwners}
                setPendingOwners={setPendingOwners}
            />

            {/* Entities Section */}
            <div style={{ marginBottom: '24px' }}>
                <Text style={{ marginBottom: '8px', display: 'block' }}>Apply tag to entities</Text>
                <EntitiesSection
                    selectedEntityUrns={selectedEntityUrns}
                    setSelectedEntityUrns={setSelectedEntityUrns}
                />
            </div>
        </Modal>
    );
};

export default CreateNewTagModal;
