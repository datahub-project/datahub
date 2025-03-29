import React, { useState, useEffect } from 'react';
import { Select, message } from 'antd';
import styled from 'styled-components';
import { useGetTagQuery } from '@src/graphql/tag.generated';
import {
    useSetTagColorMutation,
    useUpdateDescriptionMutation,
    useBatchAddOwnersMutation,
} from '@src/graphql/mutations.generated';
import { Modal, ButtonProps, ColorPicker, Input, Text } from '@components';
import { ExpandedOwner } from '@src/app/entity/shared/components/styled/ExpandedOwner/ExpandedOwner';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetSearchResultsLazyQuery } from '@src/graphql/search.generated';
import { useListOwnershipTypesQuery } from '@src/graphql/ownership.generated';
import { EntityType, OwnerEntityType } from '@src/types.generated';
import { OwnerLabel } from '@src/app/shared/OwnerLabel';

const FormSection = styled.div`
    margin-bottom: 16px;
`;

const OwnersSection = styled.div`
    margin-bottom: 16px;
`;

const OwnersHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 8px;
`;

const OwnersContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 8px;
`;

const SelectInput = styled(Select)`
    width: 100%;

    .ant-select-selection-item {
        display: flex;
        align-items: center;
        border-radius: 16px;
        margin: 2px;
        height: 32px;
        padding-left: 4px;
        border: none;
    }

    .ant-select-selection-item-remove {
        margin-left: 8px;
        margin-right: 8px;
        color: rgba(0, 0, 0, 0.45);
    }
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
    const [inputValue, setInputValue] = useState('');
    const [selectedOwnerUrns, setSelectedOwnerUrns] = useState<string[]>([]);
    const [isSearching, setIsSearching] = useState(false);

    // Search queries for owners - fetchPolicy set to 'no-cache' to avoid stale data issues
    const [userSearch, { data: userSearchData, loading: userSearchLoading }] = useGetSearchResultsLazyQuery({
        fetchPolicy: 'no-cache',
    });
    const [groupSearch, { data: groupSearchData, loading: groupSearchLoading }] = useGetSearchResultsLazyQuery({
        fetchPolicy: 'no-cache',
    });

    // Lazy load ownership types to improve initial loading time
    const { data: ownershipTypesData } = useListOwnershipTypesQuery({
        variables: {
            input: {},
        },
        skip: !isModalOpen, // Only fetch when modal is open
    });

    const ownershipTypes = ownershipTypesData?.listOwnershipTypes?.ownershipTypes || [];
    const defaultOwnerType = ownershipTypes.length > 0 ? ownershipTypes[0].urn : undefined;

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

    // Combine search results
    const userSearchResults = userSearchData?.search?.searchResults?.map((result) => result.entity) || [];
    const groupSearchResults = groupSearchData?.search?.searchResults?.map((result) => result.entity) || [];
    const combinedSearchResults = [...userSearchResults, ...groupSearchResults];

    // Search handlers with debounce to reduce API calls
    const handleSearch = (type: EntityType, text: string, searchQuery: any) => {
        if (!text || text.trim().length < 2) return;

        searchQuery({
            variables: {
                input: {
                    type,
                    query: text,
                    start: 0,
                    count: 5,
                },
            },
        });
    };

    // Debounced search handler
    const handleOwnerSearch = (text: string) => {
        setInputValue(text.trim());
        setIsSearching(true);

        if (text && text.trim().length > 1) {
            handleSearch(EntityType.CorpUser, text.trim(), userSearch);
            handleSearch(EntityType.CorpGroup, text.trim(), groupSearch);
        }
    };

    // Renders a search result in the select dropdown
    const renderSearchResult = (entityItem: any) => {
        const avatarUrl =
            entityItem.type === EntityType.CorpUser ? entityItem.editableProperties?.pictureLink : undefined;
        const displayName = entityRegistry.getDisplayName(entityItem.type, entityItem);

        return (
            <Select.Option
                key={entityItem.urn}
                value={entityItem.urn}
                label={<OwnerLabel name={displayName} avatarUrl={avatarUrl} type={entityItem.type} />}
            >
                <OwnerLabel name={displayName} avatarUrl={avatarUrl} type={entityItem.type} />
            </Select.Option>
        );
    };

    // Handle select change - now stores owners as pending until save
    const handleSelectChange = (values: any) => {
        const newValues = values as string[];
        setSelectedOwnerUrns(newValues);

        // Find new owner URNs that weren't previously selected
        const newOwnerUrns = newValues.filter((urn) => !pendingOwners.some((owner) => owner.ownerUrn === urn));

        if (newOwnerUrns.length > 0 && defaultOwnerType) {
            const newPendingOwners = newOwnerUrns.map((urn) => {
                const foundEntity = combinedSearchResults.find((e) => e.urn === urn);
                const ownerEntityType =
                    foundEntity && foundEntity.type === EntityType.CorpGroup
                        ? OwnerEntityType.CorpGroup
                        : OwnerEntityType.CorpUser;

                return {
                    ownerUrn: urn,
                    ownerEntityType,
                    ownershipTypeUrn: defaultOwnerType,
                };
            });

            setPendingOwners([...pendingOwners, ...newPendingOwners]);
        }

        // Handle removed owners
        if (newValues.length < selectedOwnerUrns.length) {
            const removedUrns = selectedOwnerUrns.filter((urn) => !newValues.includes(urn));
            const updatedPendingOwners = pendingOwners.filter((owner) => !removedUrns.includes(owner.ownerUrn));
            setPendingOwners(updatedPendingOwners);
        }
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

    // Loading state for the select
    const isSelectLoading = isSearching && (userSearchLoading || groupSearchLoading);

    // Simplified conditional content for notFoundContent
    let notFoundContent: React.ReactNode = null;
    if (isSelectLoading) {
        notFoundContent = 'Loading...';
    } else if (inputValue && combinedSearchResults.length === 0) {
        notFoundContent = 'No results found';
    }

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

                <OwnersSection>
                    <OwnersHeader>
                        <Text>Owners</Text>
                    </OwnersHeader>
                    <FormSection>
                        <SelectInput
                            mode="multiple"
                            placeholder="Search for users or groups"
                            showSearch
                            filterOption={false}
                            onSearch={handleOwnerSearch}
                            onChange={handleSelectChange}
                            value={selectedOwnerUrns}
                            loading={isSelectLoading}
                            notFoundContent={notFoundContent}
                            optionLabelProp="label"
                        >
                            {combinedSearchResults.map((entity) => renderSearchResult(entity))}
                        </SelectInput>
                    </FormSection>
                    <OwnersContainer>
                        {owners && owners.length > 0 ? (
                            owners.map((ownerItem) => (
                                <ExpandedOwner
                                    key={ownerItem.owner?.urn}
                                    entityUrn={tagUrn}
                                    owner={ownerItem}
                                    hidePopOver
                                />
                            ))
                        ) : (
                            <div>No owners assigned</div>
                        )}
                    </OwnersContainer>
                </OwnersSection>
            </div>
        </Modal>
    );
};

export { ManageTag };
