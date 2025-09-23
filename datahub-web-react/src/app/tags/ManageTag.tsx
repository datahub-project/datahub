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
    useUpdateDescriptionMutation,
} from '@src/graphql/mutations.generated';
import { useGetTagQuery } from '@src/graphql/tag.generated';
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
    const [updateDescriptionMutation] = useUpdateDescriptionMutation();
    const [batchAddOwnersMutation] = useBatchAddOwnersMutation();
    const [batchRemoveOwnersMutation] = useBatchRemoveOwnersMutation();

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

    const { defaultOwnershipType } = useOwnershipTypes();

    const onChangeOwners = (newOwners: OwnerType[]) => {
        setPendingOwners(newOwners.map((owner) => convertOwnerToPendingOwner(owner, defaultOwnershipType)));
    };

    const hasOwnerFieldChanges = useCallback(() => {
        if (pendingOwners.length !== owners.length) return true;

        const pendingOwnersSet = new Set(pendingOwners.map((owner) => owner.ownerUrn));
        const existingOwnersSet = new Set(owners.map((owner) => owner.owner.urn));

        return pendingOwnersSet.symmetricDifference(existingOwnersSet).size > 0;
    }, [pendingOwners, owners]);

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

    const handleDescriptionChange: React.Dispatch<React.SetStateAction<string>> = (value) => {
        if (typeof value === 'function') {
            setDescription(value);
        } else {
            setDescription(value);
        }
    };

    // Check if anything has changed
    const hasChanges = () => {
        return colorValue !== originalColor || description !== originalDescription || hasOwnerFieldChanges();
    };

    const handleReset = () => {
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
            disabled: !hasChanges(),
            buttonDataTestId: 'update-tag-button',
        },
    ];

    // Only render modal if isModalOpen is true
    if (!isModalOpen) {
        return null;
    }

    // Dynamic modal title
    const modalTitle = tagDisplayName ? `Edit Tag: ${tagDisplayName}` : 'Edit Tag';

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
                        label="Description"
                        value={description}
                        setValue={handleDescriptionChange}
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
