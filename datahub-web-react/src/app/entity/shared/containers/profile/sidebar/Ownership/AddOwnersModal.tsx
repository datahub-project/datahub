import React, { useEffect, useRef, useState } from 'react';
import { Button, Form, message, Modal, Select, Tag, Typography } from 'antd';
import styled from 'styled-components';

import { CorpUser, Entity, EntityType, OwnerEntityType, OwnershipType } from '../../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import analytics, { EventType, EntityActionType } from '../../../../../../analytics';
import { OWNERSHIP_DISPLAY_TYPES } from './ownershipUtils';
import { useAddOwnersMutation } from '../../../../../../../graphql/mutations.generated';
import { useGetSearchResultsLazyQuery } from '../../../../../../../graphql/search.generated';
import { useGetRecommendations } from '../../../../../../shared/recommendation';
import { OwnerLabel } from '../../../../../../shared/OwnerLabel';

const SelectInput = styled(Select)`
    > .ant-select-selector {
        height: 36px;
    }
`;

const StyleTag = styled(Tag)`
    padding: 0px 7px 0px 0px;
    margin-right: 3px;
    display: flex;
    justify-content: start;
    align-items: center;
`;

type Props = {
    urn: string;
    type: EntityType;
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

export const AddOwnersModal = ({ urn, type, hideOwnerType, defaultOwnerType, onCloseModal, refetch }: Props) => {
    const entityRegistry = useEntityRegistry();
    const [inputValue, setInputValue] = useState('');
    const [addOwnersMutation] = useAddOwnersMutation();
    const ownershipTypes = OWNERSHIP_DISPLAY_TYPES;
    const [selectedOwners, setSelectedOwners] = useState<SelectedOwner[]>([]);
    const [selectedOwnerType, setSelectedOwnerType] = useState<OwnershipType>(defaultOwnerType || OwnershipType.None);

    // User and group dropdown search results!
    const [userSearch, { data: userSearchData }] = useGetSearchResultsLazyQuery();
    const [groupSearch, { data: groupSearchData }] = useGetSearchResultsLazyQuery();
    const userSearchResults = userSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];
    const groupSearchResults = groupSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];
    const combinedSearchResults = [...userSearchResults, ...groupSearchResults];
    const [recommendedData] = useGetRecommendations([EntityType.CorpGroup, EntityType.CorpUser]);
    const inputEl = useRef(null);

    useEffect(() => {
        if (ownershipTypes) {
            setSelectedOwnerType(ownershipTypes[0].type);
        }
    }, [ownershipTypes]);

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
    const renderSearchResult = (entity: Entity) => {
        const avatarUrl =
            entity.type === EntityType.CorpUser
                ? (entity as CorpUser).editableProperties?.pictureLink || undefined
                : undefined;
        const displayName = entityRegistry.getDisplayName(entity.type, entity);
        return (
            <Select.Option value={entity.urn} key={entity.urn}>
                <OwnerLabel name={displayName} avatarUrl={avatarUrl} type={entity.type} />
            </Select.Option>
        );
    };

    const ownerResult = !inputValue || inputValue.length === 0 ? recommendedData : combinedSearchResults;

    const ownerSearchOptions = ownerResult?.map((result) => {
        return renderSearchResult(result);
    });

    const onModalClose = () => {
        setInputValue('');
        setSelectedOwners([]);
        setSelectedOwnerType(defaultOwnerType || OwnershipType.None);
        onCloseModal();
    };

    /**
     * When a owner search result is selected, add the new owner  to the selectedOwners
     * value: {ownerUrn: string, ownerEntityType: EntityType}
     */
    const onSelectOwner = (selectedValue: { key: string; label: React.ReactNode; value: string }) => {
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
        const filteredActors = ownerResult
            ?.filter((entity) => entity.urn === selectedValue.value)
            .map((entity) => entity);
        if (filteredActors?.length) {
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
        setInputValue('');
        const newValues = selectedOwners.filter((owner) => owner.label !== selectedValue.value);
        setSelectedOwners(newValues);
    };

    // When a owner type is selected, set the type as selected type.
    const onSelectOwnerType = (newType: OwnershipType) => {
        setSelectedOwnerType(newType);
    };

    const tagRender = (props) => {
        // eslint-disable-next-line react/prop-types
        const { label, closable, onClose } = props;
        const onPreventMouseDown = (event) => {
            event.preventDefault();
            event.stopPropagation();
        };
        return (
            <StyleTag onMouseDown={onPreventMouseDown} closable={closable} onClose={onClose}>
                {label}
            </StyleTag>
        );
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

    function handleBlur() {
        setInputValue('');
    }

    return (
        <Modal
            title="Add Owners"
            visible
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
                <Form.Item key="owners" name="owners" label={<Typography.Text strong>Owner</Typography.Text>}>
                    <Typography.Paragraph>Find a user or group</Typography.Paragraph>
                    <Form.Item name="owner">
                        <SelectInput
                            labelInValue
                            autoFocus
                            defaultOpen
                            mode="multiple"
                            ref={inputEl}
                            placeholder="Search for users or groups..."
                            showSearch
                            filterOption={false}
                            defaultActiveFirstOption={false}
                            onSelect={(asset: any) => onSelectOwner(asset)}
                            onDeselect={(asset: any) => onDeselectOwner(asset)}
                            onSearch={(value: string) => {
                                // eslint-disable-next-line react/prop-types
                                handleActorSearch(value.trim());
                                // eslint-disable-next-line react/prop-types
                                setInputValue(value.trim());
                            }}
                            tagRender={tagRender}
                            onBlur={handleBlur}
                            value={selectedOwners}
                        >
                            {ownerSearchOptions}
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
