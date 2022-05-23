import React, { useEffect, useState } from 'react';
import { Button, Form, message, Modal, Select, Typography } from 'antd';
import styled from 'styled-components';
import { Link } from 'react-router-dom';

import {
    CorpUser,
    EntityType,
    OwnerEntityType,
    OwnershipType,
    SearchResult,
} from '../../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { CustomAvatar } from '../../../../../../shared/avatar';
import analytics, { EventType, EntityActionType } from '../../../../../../analytics';
import { OWNERSHIP_DISPLAY_TYPES } from './ownershipUtils';
import { useAddOwnersMutation } from '../../../../../../../graphql/mutations.generated';
import { useGetSearchResultsLazyQuery } from '../../../../../../../graphql/search.generated';
import SelectedOwnerTag from '../../../../../../shared/SelectedOwnerTag';

const SearchResultContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 2px;
`;

const SearchResultContent = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
`;

const SearchResultDisplayName = styled.div``;

type Props = {
    urn: string;
    type: EntityType;
    visible: boolean;
    defaultOwnerType?: OwnershipType;
    hideOwnerType?: boolean | undefined;
    onCloseModal: () => void;
    refetch?: () => Promise<any>;
};

type SelectedOwner = {
    ownerEntityType: OwnerEntityType;
    ownerUrn: string;
};

export const AddOwnersModal = ({
    urn,
    type,
    visible,
    hideOwnerType,
    defaultOwnerType,
    onCloseModal,
    refetch,
}: Props) => {
    const entityRegistry = useEntityRegistry();
    const [addOwnersMutation] = useAddOwnersMutation();
    const ownershipTypes = OWNERSHIP_DISPLAY_TYPES;
    const [selectedOwners, setSelectedOwners] = useState<SelectedOwner[]>([]);
    const [selectedOwnerType, setSelectedOwnerType] = useState<OwnershipType>(defaultOwnerType || OwnershipType.None);

    // User and group dropdown search results!
    const [userSearch, { data: userSearchData }] = useGetSearchResultsLazyQuery();
    const [groupSearch, { data: groupSearchData }] = useGetSearchResultsLazyQuery();
    const userSearchResults = userSearchData?.search?.searchResults || [];
    const groupSearchResults = groupSearchData?.search?.searchResults || [];
    const combinedSearchResults = [...userSearchResults, ...groupSearchResults];

    useEffect(() => {
        if (ownershipTypes) {
            setSelectedOwnerType(ownershipTypes[0].type);
        }
    }, [ownershipTypes]);

    // When a owner search result is selected, add the new owner  to the selectedOwners
    const onSelectOwner = (newOwnerUrnId: string) => {
        const filteredActors = combinedSearchResults
            .filter((result) => result.entity.urn === newOwnerUrnId)
            .map((result) => result.entity);
        if (filteredActors.length) {
            const actor = filteredActors[0];
            const ownerEntityType =
                actor && actor.type === EntityType.CorpGroup ? OwnerEntityType.CorpGroup : OwnerEntityType.CorpUser;
            const newOwners = [
                ...selectedOwners,
                {
                    ownerUrn: newOwnerUrnId,
                    ownerEntityType,
                },
            ];
            setSelectedOwners(newOwners);
        }
    };

    // When a owner search result is deselected, remove the urn from the Owners
    const onDeselectOwner = (selectedOwnerUrnId: string) => {
        const newOwners = selectedOwners.filter((owner) => owner.ownerUrn !== selectedOwnerUrnId);
        setSelectedOwners(newOwners);
    };

    // When a owner type is selected, set the type as selected type.
    const onSelectOwnerType = (newType: OwnershipType) => {
        setSelectedOwnerType(newType);
    };

    // Invokes the search API as the owner types
    const handleSearch = (entityType: EntityType, text: string, searchQuery: any) => {
        if (text.length > 2) {
            searchQuery({
                variables: {
                    input: {
                        type: entityType,
                        query: text,
                        start: 0,
                        count: 5,
                    },
                },
            });
        }
    };

    // Invokes the user search API for both users and groups.
    const handleActorSearch = (text: string) => {
        handleSearch(EntityType.CorpUser, text, userSearch);
        handleSearch(EntityType.CorpGroup, text, groupSearch);
    };

    // Renders a search result in the select dropdown.
    const renderSearchResult = (result: SearchResult) => {
        const avatarUrl =
            result.entity.type === EntityType.CorpUser
                ? (result.entity as CorpUser).editableProperties?.pictureLink || undefined
                : undefined;
        const displayName = entityRegistry.getDisplayName(result.entity.type, result.entity);
        return (
            <SearchResultContainer>
                <Link
                    target="_blank"
                    rel="noopener noreferrer"
                    to={() => `/${entityRegistry.getPathName(result.entity.type)}/${result.entity.urn}`}
                >
                    <SearchResultContent>
                        <CustomAvatar
                            size={24}
                            name={displayName}
                            photoUrl={avatarUrl}
                            isGroup={result.entity.type === EntityType.CorpGroup}
                        />
                        <SearchResultDisplayName>
                            <div>{displayName}</div>
                        </SearchResultDisplayName>
                    </SearchResultContent>
                </Link>
            </SearchResultContainer>
        );
    };

    const onModalClose = () => {
        onCloseModal();
        setSelectedOwners([]);
        setSelectedOwnerType(defaultOwnerType || OwnershipType.None);
    };

    // Function to handle the modal action's
    const onOk = async () => {
        if (selectedOwners.length === 0) {
            return;
        }
        const inputs = selectedOwners.map((selectedActor) => {
            return { ...selectedActor, type: selectedOwnerType };
        });
        try {
            await addOwnersMutation({
                variables: {
                    input: {
                        owners: inputs,
                        resourceUrn: urn,
                    },
                },
            });
            message.success({ content: "Owner's Added", duration: 2 });
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: EntityActionType.UpdateOwnership,
                entityType: type,
                entityUrn: urn,
            });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to add owners: \n ${e.message || ''}`, duration: 3 });
            }
        } finally {
            refetch?.();
            onModalClose();
        }
    };

    console.log('selectedOwners::', selectedOwners);

    return (
        <Modal
            title="Add Owners"
            visible={visible}
            onCancel={onModalClose}
            keyboard
            footer={
                <>
                    <Button onClick={onModalClose} type="text">
                        Cancel
                    </Button>
                    <Button id="addOwnerButton" disabled={selectedOwners.length === 0} onClick={onOk}>
                        Add
                    </Button>
                </>
            }
        >
            <Form layout="vertical" colon={false}>
                <Form.Item label={<Typography.Text strong>Owner</Typography.Text>}>
                    <Typography.Paragraph>Find a user or group</Typography.Paragraph>
                    <Form.Item name="owner">
                        <Select
                            autoFocus
                            mode="multiple"
                            filterOption={false}
                            placeholder="Search for users or groups..."
                            onSelect={(asset: any) => onSelectOwner(asset)}
                            onDeselect={(asset: any) => onDeselectOwner(asset)}
                            onSearch={handleActorSearch}
                            tagRender={(tagProps) => (
                                <SelectedOwnerTag
                                    closable={tagProps.closable}
                                    onClose={tagProps.onClose}
                                    label={tagProps.label}
                                />
                            )}
                        >
                            {combinedSearchResults?.map((result) => (
                                <Select.Option key={result?.entity?.urn} value={result.entity.urn}>
                                    {renderSearchResult(result)}
                                </Select.Option>
                            ))}
                        </Select>
                    </Form.Item>
                </Form.Item>
                {!hideOwnerType && (
                    <Form.Item label={<Typography.Text strong>Type</Typography.Text>}>
                        <Typography.Paragraph>Choose an owner type</Typography.Paragraph>
                        <Form.Item name="type">
                            <Select
                                defaultValue={selectedOwnerType}
                                value={selectedOwnerType}
                                onChange={onSelectOwnerType}
                            >
                                {ownershipTypes.map((ownerType) => (
                                    <Select.Option key={ownerType.type} value={ownerType.type}>
                                        <Typography.Text>{ownerType.name}</Typography.Text>
                                        <div>
                                            <Typography.Paragraph style={{ wordBreak: 'break-all' }} type="secondary">
                                                {ownerType.description}
                                            </Typography.Paragraph>
                                        </div>
                                    </Select.Option>
                                ))}
                            </Select>
                        </Form.Item>
                    </Form.Item>
                )}
            </Form>
        </Modal>
    );
};
