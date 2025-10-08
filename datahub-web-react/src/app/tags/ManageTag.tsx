import { ColorPicker, Input, Modal } from '@components';
import { message } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { ModalButton } from '@components/components/Modal/Modal';

import OwnersSection from '@app/sharedV2/owners/OwnersSection';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useBatchAddOwnersMutation, useSetTagColorMutation } from '@src/graphql/mutations.generated';
import { useGetTagQuery, useUpdateTagMutation } from '@src/graphql/tag.generated';
import { EntityType, OwnerEntityType } from '@src/types.generated';

const FormSection = styled.div`
    margin-bottom: 16px;
`;

interface Props {
    tagUrn: string;
    onClose: () => void;
    onSave?: () => void;
    isModalOpen?: boolean;
}

// Interface for pending owner
interface PendingOwner {
    ownerUrn: string;
    ownerEntityType: OwnerEntityType;
    ownershipTypeUrn: string;
}

const ManageTag = ({ tagUrn, onClose, onSave, isModalOpen = false }: Props) => {
    const entityRegistry = useEntityRegistry();
    const { data, loading, refetch } = useGetTagQuery({
        variables: { urn: tagUrn },
        fetchPolicy: 'cache-first',
    });

    const [setTagColorMutation] = useSetTagColorMutation();
    const [batchAddOwnersMutation] = useBatchAddOwnersMutation();
    const [updateTagMutation] = useUpdateTagMutation();

    // State to track values
    const [colorValue, setColorValue] = useState('#1890ff');
    const [originalColor, setOriginalColor] = useState('');

    // Tag name state (editable)
    const [tagName, setTagName] = useState('');
    const [originalTagName, setOriginalTagName] = useState('');

    // Tag name for display purposes only
    const [tagDisplayName, setTagDisplayName] = useState('');

    // Description state
    const [description, setDescription] = useState('');
    const [originalDescription, setOriginalDescription] = useState('');

    // Owners state
    const [owners, setOwners] = useState<any[]>([]);
    const [pendingOwners, setPendingOwners] = useState<PendingOwner[]>([]);
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);

    const onChangeOwners = (newOwners: PendingOwner[]) => {
        setPendingOwners(newOwners);
    };

    // When data loads, set the initial values
    useEffect(() => {
        if (data?.tag) {
            const tagColor = data.tag.properties?.colorHex || '#1890ff';
            setColorValue(tagColor);
            setOriginalColor(tagColor);

            // Get the tag name for display and editing
            const displayName = entityRegistry.getDisplayName(EntityType.Tag, data.tag) || data.tag.name || '';
            const tagNameValue = data.tag.properties?.name || data.tag.name || '';
            setTagDisplayName(displayName);
            setTagName(tagNameValue);
            setOriginalTagName(tagNameValue);

            // Get the description
            const desc = data.tag.properties?.description || '';
            setDescription(desc);
            setOriginalDescription(desc);

            // Set owners
            const tagOwners = data.tag.ownership?.owners || [];
            setOwners(tagOwners);
        }
    }, [data, entityRegistry]);

    // Handler functions
    const handleColorChange = (color: string) => {
        setColorValue(color);
    };

    // Check if anything has changed
    const hasChanges = () => {
        return (
            tagName !== originalTagName ||
            colorValue !== originalColor ||
            description !== originalDescription ||
            pendingOwners.length > 0
        );
    };

    const handleReset = () => {
        setTagName(originalTagName);
        setColorValue(originalColor);
        setDescription(originalDescription);
        setPendingOwners([]);
        setSelectedOwnerUrns([]);
    };

    // Save everything together
    const handleSave = async () => {
        try {
            // Validate required fields
            if (!tagName.trim()) {
                message.error({
                    content: 'Tag name is required',
                    key: 'tagUpdate',
                    duration: 3,
                });
                return;
            }

            message.loading({ content: 'Saving changes...', key: 'tagUpdate' });

            // Track if we made any successful changes
            let changesMade = false;

            // Update color if changed
            if (colorValue !== originalColor) {
                try {
                    await setTagColorMutation({
                        variables: {
                            urn: tagUrn,
                            colorHex: colorValue,
                        },
                    });
                    changesMade = true;
                } catch (colorError) {
                    console.error('Error updating color:', colorError);
                    throw new Error(
                        `Failed to update color: ${
                            colorError instanceof Error ? colorError.message : String(colorError)
                        }`,
                    );
                }
            }

            // Update tag name and/or description if changed
            if (tagName !== originalTagName || description !== originalDescription) {
                try {
                    await updateTagMutation({
                        variables: {
                            urn: tagUrn,
                            input: {
                                urn: tagUrn,
                                name: tagName,
                                description: description || undefined,
                            },
                        },
                    });
                    changesMade = true;
                } catch (tagUpdateError) {
                    console.error('Error updating tag:', tagUpdateError);
                    throw new Error(
                        `Failed to update tag: ${
                            tagUpdateError instanceof Error ? tagUpdateError.message : String(tagUpdateError)
                        }`,
                    );
                }
            }

            // Add pending owners if any
            if (pendingOwners.length > 0) {
                try {
                    await batchAddOwnersMutation({
                        variables: {
                            input: {
                                owners: pendingOwners,
                                resources: [{ resourceUrn: tagUrn }],
                            },
                        },
                    });
                    changesMade = true;
                } catch (ownerError) {
                    console.error('Error adding owners:', ownerError);
                    throw new Error(
                        `Failed to add owners: ${
                            ownerError instanceof Error ? ownerError.message : String(ownerError)
                        }`,
                    );
                }
            }

            if (changesMade) {
                message.success({
                    content: 'Tag updated successfully!',
                    key: 'tagUpdate',
                    duration: 2,
                });
            }

            // Refetch to update the UI
            await refetch();

            // Call the onSave callback if provided
            if (onSave) {
                onSave();
            }

            // Close the modal
            onClose();
        } catch (error: unknown) {
            const errorMessage = error instanceof Error ? error.message : 'Unknown error';
            message.error({
                content: `Failed to update tag: ${errorMessage}`,
                key: 'tagUpdate',
                duration: 3,
            });
        }
    };

    if (loading) {
        return <div>Loading...</div>;
    }

    const buttons: ModalButton[] = [
        {
            text: 'Cancel',
            color: 'violet',
            variant: 'text',
            onClick: onClose,
        },
        {
            text: 'Reset',
            color: 'violet',
            variant: 'outline',
            onClick: handleReset,
            disabled: !hasChanges(),
        },
        {
            text: 'Save',
            color: 'violet',
            variant: 'filled',
            onClick: handleSave,
            disabled: !hasChanges() || !tagName.trim(),
            buttonDataTestId: 'update-tag-button',
        },
    ];

    // Only render modal if isModalOpen is true
    if (!isModalOpen) {
        return null;
    }

    // Dynamic modal title
    const modalTitle = tagDisplayName ? `Edit Tag` : 'Edit Tag';

    return (
        <Modal
            title={modalTitle}
            onCancel={onClose}
            buttons={buttons}
            open={isModalOpen}
            centered
            width={400}
            dataTestId="edit-tag-modal"
        >
            <div>
                <FormSection>
                    <Input
                        label="Name"
                        value={tagName}
                        setValue={setTagName}
                        placeholder="Enter tag name"
                        isRequired
                        data-testid="tag-name-field"
                    />
                </FormSection>

                <FormSection>
                    <Input
                        label="Description"
                        value={description}
                        setValue={setDescription}
                        placeholder="Tag description"
                        type="textarea"
                        data-testid="tag-description-field"
                    />
                </FormSection>

                <FormSection>
                    <ColorPicker initialColor={colorValue} onChange={handleColorChange} label="Color" />
                </FormSection>

                <OwnersSection
                    selectedOwnerUrns={selectedOwnerUrns}
                    setSelectedOwnerUrns={setSelectedOwnerUrns}
                    existingOwners={owners}
                    onChange={onChangeOwners}
                    sourceRefetch={refetch}
                    isEditForm
                />
            </div>
        </Modal>
    );
};

export { ManageTag };
