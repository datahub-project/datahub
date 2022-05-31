import React, { useEffect, useState } from 'react';
import { Button, Form, message, Modal, Select, Tag, Typography } from 'antd';
import styled from 'styled-components';

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

const SelectInput = styled(Select)`
    > .ant-select-selector {
        height: 36px;
    }
`;

type Props = {
    urn: string;
    type: EntityType;
    visible: boolean;
    defaultOwnerType?: OwnershipType;
    hideOwnerType?: boolean | undefined;
    onCloseModal: () => void;
    refetch?: () => Promise<any>;
};

// value: {ownerUrn: string, ownerEntityType: EntityType}
type SelectedOwner = {
    label: string;
    value;
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

    // Add owners Form
    const [form] = Form.useForm();

    useEffect(() => {
        if (ownershipTypes) {
            setSelectedOwnerType(ownershipTypes[0].type);
        }
    }, [ownershipTypes]);

    /**
     * When a owner search result is selected, add the new owner  to the selectedOwners
     * value: {ownerUrn: string, ownerEntityType: EntityType}
     */
    const onSelectOwner = (selectedValue: { key: string; label: React.ReactNode; value: string }) => {
        const filteredActors = combinedSearchResults
            .filter((result) => result.entity.urn === selectedValue.value)
            .map((result) => result.entity);
        if (filteredActors.length) {
            const actor = filteredActors[0];
            const ownerEntityType =
                actor && actor.type === EntityType.CorpGroup ? OwnerEntityType.CorpGroup : OwnerEntityType.CorpUser;
            const newValues = [
                ...selectedOwners,
                {
                    label: selectedValue.value,
                    value: {
                        ownerUrn: selectedValue.value,
                        ownerEntityType,
                    },
                },
            ];
            setSelectedOwners(newValues);
        }
    };

    // When a owner search result is deselected, remove the Owner
    const onDeselectOwner = (selectedValue: { key: string; label: React.ReactNode; value: string }) => {
        const newValues = selectedOwners.filter((owner) => owner.label !== selectedValue.value);
        setSelectedOwners(newValues);
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
                <SearchResultContent>
                    <CustomAvatar
                        size={24}
                        name={displayName}
                        photoUrl={avatarUrl}
                        isGroup={result.entity.type === EntityType.CorpGroup}
                    />
                    <div>{displayName}</div>
                </SearchResultContent>
            </SearchResultContainer>
        );
    };

    const onModalClose = () => {
        setSelectedOwners([]);
        setSelectedOwnerType(defaultOwnerType || OwnershipType.None);
        form.resetFields();
        onCloseModal();
    };

    // Function to handle the modal action's
    const onOk = async () => {
        if (selectedOwners.length === 0) {
            return;
        }
        const inputs = selectedOwners.map((selectedActor) => {
            const input = {
                ownerUrn: selectedActor.value.ownerUrn,
                ownerEntityType: selectedActor.value.ownerEntityType,
                type: selectedOwnerType,
            };
            return input;
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
            message.success({ content: 'Owners Added', duration: 2 });
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

    const tagRender = (props) => {
        // eslint-disable-next-line react/prop-types
        const { label, closable, onClose } = props;
        const onPreventMouseDown = (event) => {
            event.preventDefault();
            event.stopPropagation();
        };
        return (
            <Tag
                onMouseDown={onPreventMouseDown}
                closable={closable}
                onClose={onClose}
                style={{
                    padding: '0px 7px 0px 0px',
                    marginRight: 3,
                    display: 'flex',
                    justifyContent: 'start',
                    alignItems: 'center',
                }}
            >
                {label}
            </Tag>
        );
    };

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
            <Form layout="vertical" form={form} colon={false}>
                <Form.Item key="owners" name="owners" label={<Typography.Text strong>Owner</Typography.Text>}>
                    <Typography.Paragraph>Find a user or group</Typography.Paragraph>
                    <Form.Item name="owner">
                        <SelectInput
                            labelInValue
                            value={selectedOwners}
                            autoFocus
                            mode="multiple"
                            filterOption={false}
                            placeholder="Search for users or groups..."
                            onSelect={(asset: any) => onSelectOwner(asset)}
                            onDeselect={(asset: any) => onDeselectOwner(asset)}
                            onSearch={handleActorSearch}
                            tagRender={tagRender}
                        >
                            {combinedSearchResults?.map((result) => (
                                <Select.Option key={result?.entity?.urn} value={result.entity.urn}>
                                    {renderSearchResult(result)}
                                </Select.Option>
                            ))}
                        </SelectInput>
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
