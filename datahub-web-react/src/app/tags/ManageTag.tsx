import { ColorPicker, Input, Modal } from '@components';
import { message } from 'antd';
import React, { useCallback, useEffect, useState } from 'react';
import styled from 'styled-components';

import { ModalButton } from '@components/components/Modal/Modal';

import OwnersSection from '@app/sharedV2/owners/OwnersSectionV2';
import useOwnershipTypes from '@app/sharedV2/owners/hooks/useOwnershipTypes';
import { convertOwnerToPendingOwner } from '@app/sharedV2/owners/utils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import {
    useBatchAddOwnersMutation,
    useBatchRemoveOwnersMutation,
    useSetTagColorMutation,
} from '@src/graphql/mutations.generated';
import { useGetTagQuery, useUpdateTagMutation } from '@src/graphql/tag.generated';
import { EntityType, OwnerEntityType, OwnerType } from '@src/types.generated';

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
    ownershipTypeUrn?: string;
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
    const [batchRemoveOwnersMutation] = useBatchRemoveOwnersMutation();

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

    const { defaultOwnershipType } = useOwnershipTypes();

    const onChangeOwners = (newOwners: OwnerType[]) => {
        setPendingOwners(newOwners.map((owner) => convertOwnerToPendingOwner(owner, defaultOwnershipType)));
    };

    const hasOwnerFieldChanges = useCallback(() => {
        const pendingOwnersUrns = pendingOwners.map((owner) => owner.ownerUrn);
        const existingOwnersUrns = owners.map((owner) => owner.owner.urn);

        if (pendingOwnersUrns.length !== existingOwnersUrns.length) {
            return true;
        }

        return pendingOwnersUrns.some((urn) => !existingOwnersUrns.includes(urn));
    }, [pendingOwners, owners]);

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
            setPendingOwners(
                tagOwners.map((tagOwner) =>
                    convertOwnerToPendingOwner(
                        { urn: tagOwner.owner.urn, type: tagOwner.owner.type },
                        defaultOwnershipType,
                    ),
                ),
            );
            setSelectedOwnerUrns(tagOwners.map((owner) => owner.owner.urn));
        }
    }, [data, entityRegistry, defaultOwnershipType]);

    // Handler functions
    const handleColorChange = (color: string) => {
        setColorValue(color);
    };

    // Check if anything has changed
    const hasChanges = () => {
        return colorValue !== originalColor || description !== originalDescription || hasOwnerFieldChanges();
    };

    const handleReset = () => {
        setTagName(originalTagName);
        setColorValue(originalColor);
        setDescription(originalDescription);
        setPendingOwners(
            owners.map((owner) =>
                convertOwnerToPendingOwner({ urn: owner.owner.urn, type: owner.owner.type }, defaultOwnershipType),
            ),
        );
        setSelectedOwnerUrns(owners.map((owner) => owner.owner.urn));
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
            const existingOwnersUrns = owners.map((owner) => owner.owner.urn);
            const ownersToAdd = pendingOwners.filter((owner) => !existingOwnersUrns.includes(owner.ownerUrn));

            const pendingOwnersUrns = pendingOwners.map((owner) => owner.ownerUrn);
            const ownersUrnsToRemove = existingOwnersUrns.filter(
                (existingOwnerUrn) => !pendingOwnersUrns.includes(existingOwnerUrn),
            );

            try {
                if (ownersToAdd.length) {
                    await batchAddOwnersMutation({
                        variables: {
                            input: {
                                owners: ownersToAdd,
                                resources: [{ resourceUrn: tagUrn }],
                            },
                        },
                    });
                }
                if (ownersUrnsToRemove.length) {
                    await batchRemoveOwnersMutation({
                        variables: {
                            input: {
                                ownerUrns: ownersUrnsToRemove,
                                resources: [{ resourceUrn: tagUrn }],
                            },
                        },
                    });
                }
                changesMade = true;
            } catch (ownerError) {
                console.error('Error updating owners:', ownerError);
                throw new Error(
                    `Failed to update owners: ${ownerError instanceof Error ? ownerError.message : String(ownerError)}`,
                );
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
                    isEditForm
                />
            </div>
        </Modal>
    );
};

export { ManageTag };
