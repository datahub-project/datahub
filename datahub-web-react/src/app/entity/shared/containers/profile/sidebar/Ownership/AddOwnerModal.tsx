import { Button, Form, message, Modal, Select, Tag, Typography } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { useAddOwnerMutation } from '../../../../../../../graphql/mutations.generated';
import { useGetSearchResultsLazyQuery } from '../../../../../../../graphql/search.generated';
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

const SearchResultContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px;
`;

const SearchResultContent = styled.div`
    display: flex;
    justify-content: start;
    align-items: center;
`;

const SearchResultDisplayName = styled.div`
    margin-left: 12px;
`;

type Props = {
    urn: string;
    type: EntityType;
    visible: boolean;
    defaultOwnerType?: OwnershipType;
    hideOwnerType?: boolean | undefined;
    onClose: () => void;
    refetch?: () => Promise<any>;
};

type SelectedActor = {
    displayName: string;
    type: EntityType;
    urn: string;
};

export const AddOwnerModal = ({ urn, type, visible, hideOwnerType, defaultOwnerType, onClose, refetch }: Props) => {
    const entityRegistry = useEntityRegistry();
    const [selectedActor, setSelectedActor] = useState<SelectedActor | undefined>(undefined);
    const [selectedOwnerType, setSelectedOwnerType] = useState<OwnershipType>(defaultOwnerType || OwnershipType.None);
    const [userSearch, { data: userSearchData }] = useGetSearchResultsLazyQuery();
    const [groupSearch, { data: groupSearchData }] = useGetSearchResultsLazyQuery();
    const [addOwnerMutation] = useAddOwnerMutation();

    // User and group dropdown search results!
    const userSearchResults = userSearchData?.search?.searchResults || [];
    const groupSearchResults = groupSearchData?.search?.searchResults || [];
    const combinedSearchResults = [...userSearchResults, ...groupSearchResults];

    const inputEl = useRef(null);

    const onOk = async () => {
        if (!selectedActor) {
            return;
        }
        try {
            const ownerEntityType =
                selectedActor.type === EntityType.CorpGroup ? OwnerEntityType.CorpGroup : OwnerEntityType.CorpUser;
            await addOwnerMutation({
                variables: {
                    input: {
                        ownerUrn: selectedActor.urn,
                        type: selectedOwnerType,
                        resourceUrn: urn,
                        ownerEntityType,
                    },
                },
            });
            message.success({ content: 'Owner Added', duration: 2 });
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: EntityActionType.UpdateOwnership,
                entityType: type,
                entityUrn: urn,
            });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to add owner: \n ${e.message || ''}`, duration: 3 });
            }
        }
        setSelectedActor(undefined);
        refetch?.();
        onClose();
    };

    // When a user search result is selected, set the urn as the selected urn.
    const onSelectActor = (newUrn: string) => {
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
        const filteredActors = combinedSearchResults
            .filter((result) => result.entity.urn === newUrn)
            .map((result) => result.entity);
        if (filteredActors.length) {
            const actor = filteredActors[0];
            setSelectedActor({
                displayName: entityRegistry.getDisplayName(actor.type, actor),
                type: actor.type,
                urn: actor.urn,
            });
        }
    };

    // When a user search result is selected, set the urn as the selected urn.
    const onDeselectActor = (_: string) => {
        setSelectedActor(undefined);
    };

    // When a user search result is selected, set the urn as the selected urn.
    const onSelectOwnerType = (newType: OwnershipType) => {
        setSelectedOwnerType(newType);
    };

    // Invokes the search API as the user types
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
    // TODO: replace with multi entity search.
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
                        size={32}
                        name={displayName}
                        photoUrl={avatarUrl}
                        isGroup={result.entity.type === EntityType.CorpGroup}
                    />
                    <SearchResultDisplayName>
                        <div>
                            <Typography.Text type="secondary">
                                {entityRegistry.getEntityName(result.entity.type)}
                            </Typography.Text>
                        </div>
                        <div>{displayName}</div>
                    </SearchResultDisplayName>
                </SearchResultContent>
                <Link
                    target="_blank"
                    rel="noopener noreferrer"
                    to={() => `/${entityRegistry.getPathName(result.entity.type)}/${result.entity.urn}`}
                >
                    View
                </Link>{' '}
            </SearchResultContainer>
        );
    };

    const selectValue = (selectedActor && [selectedActor.displayName]) || [];
    const ownershipTypes = OWNERSHIP_DISPLAY_TYPES;

    useEffect(() => {
        if (ownershipTypes) {
            setSelectedOwnerType(ownershipTypes[0].type);
        }
    }, [ownershipTypes]);

    // Handle the Enter press
    // TODO: Allow user to be selected prior to executed the save.
    // useEnterKeyListener({
    //    querySelectorToExecuteClick: selectedActor && '#addOwnerButton',
    // });

    return (
        <Modal
            title="Add Owner"
            visible={visible}
            onCancel={onClose}
            keyboard
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button id="addOwnerButton" disabled={selectedActor === undefined} onClick={onOk}>
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
                            filterOption={false}
                            value={selectValue}
                            mode="multiple"
                            ref={inputEl}
                            placeholder="Search for users or groups..."
                            onSelect={(actorUrn: any) => onSelectActor(actorUrn)}
                            onDeselect={(actorUrn: any) => onDeselectActor(actorUrn)}
                            onSearch={handleActorSearch}
                            tagRender={(tagProps) => <Tag>{tagProps.value}</Tag>}
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
