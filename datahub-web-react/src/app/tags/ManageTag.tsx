import { ButtonProps, ColorPicker, Input, Modal } from '@components';
import { message } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import OwnersSection from '@app/sharedV2/owners/OwnersSection';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import {
    useBatchAddOwnersMutation,
    useSetTagColorMutation,
    useUpdateDescriptionMutation,
} from '@src/graphql/mutations.generated';
import { useGetTagQuery } from '@src/graphql/tag.generated';
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

// Define a compatible interface for modal buttons
interface ModalButton extends ButtonProps {
    text: string;
    onClick: () => void;
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
    const [updateDescriptionMutation] = useUpdateDescriptionMutation();
    const [batchAddOwnersMutation] = useBatchAddOwnersMutation();

    // State to track values
    const [colorValue, setColorValue] = useState('#1890ff');
    const [originalColor, setOriginalColor] = useState('');

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

            // Get the tag name for display
            const displayName = entityRegistry.getDisplayName(EntityType.Tag, data.tag) || data.tag.name || '';
            setTagDisplayName(displayName);

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

    const handleDescriptionChange: React.Dispatch<React.SetStateAction<string>> = (value) => {
        if (typeof value === 'function') {
            setDescription(value);
        } else {
            setDescription(value);
        }
    };

    // Check if anything has changed
    const hasChanges = () => {
        return colorValue !== originalColor || description !== originalDescription || pendingOwners.length > 0;
    };

    const handleReset = () => {
        setColorValue(originalColor);
        setDescription(originalDescription);
        setPendingOwners([]);
        setSelectedOwnerUrns([]);
    };

    // Save everything together
    const handleSave = async () => {
        try {
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

            // Update description if changed
            if (description !== originalDescription) {
                try {
                    await updateDescriptionMutation({
                        variables: {
                            input: {
                                resourceUrn: tagUrn,
                                description,
                            },
                        },
                    });
                    changesMade = true;
                } catch (descError) {
                    console.error('Error updating description:', descError);
                    throw new Error(
                        `Failed to update description: ${
                            descError instanceof Error ? descError.message : String(descError)
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
            disabled: !hasChanges(),
        },
    ];

    // Only render modal if isModalOpen is true
    if (!isModalOpen) {
        return null;
    }

    // Dynamic modal title
    const modalTitle = tagDisplayName ? `Edit Tag: ${tagDisplayName}` : 'Edit Tag';

    return (
        <Modal title={modalTitle} onCancel={onClose} buttons={buttons} open={isModalOpen} centered width={400}>
            <div>
                <FormSection>
                    <Input
                        label="Description"
                        value={description}
                        setValue={handleDescriptionChange}
                        placeholder="Tag description"
                        type="textarea"
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
