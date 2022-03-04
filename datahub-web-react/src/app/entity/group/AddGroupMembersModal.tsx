import React, { useRef, useState } from 'react';
import { message, Modal, Button, Form, Select, Tag } from 'antd';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { useAddGroupMembersMutation } from '../../../graphql/group.generated';
import { CorpUser, EntityType, SearchResult } from '../../../types.generated';
import { CustomAvatar } from '../../shared/avatar';
import { useGetSearchResultsLazyQuery } from '../../../graphql/search.generated';
import { useEntityRegistry } from '../../useEntityRegistry';

type Props = {
    urn: string;
    visible: boolean;
    onClose: () => void;
    onSubmit: () => void;
};

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

export const AddGroupMembersModal = ({ urn, visible, onClose, onSubmit }: Props) => {
    const entityRegistry = useEntityRegistry();
    const [selectedUsers, setSelectedUsers] = useState<Array<CorpUser>>([]);
    const [userSearch, { data: userSearchData }] = useGetSearchResultsLazyQuery();
    const [addGroupMembersMutation] = useAddGroupMembersMutation();
    const searchResults = userSearchData?.search?.searchResults || [];

    const inputEl = useRef(null);

    const onAdd = async () => {
        if (selectedUsers.length === 0) {
            return;
        }
        addGroupMembersMutation({
            variables: {
                groupUrn: urn,
                userUrns: selectedUsers.map((user) => user.urn),
            },
        })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to add group members!: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                message.success({
                    content: `Group members added!`,
                    duration: 3,
                });
                onSubmit();
                setSelectedUsers([]);
            });
        onClose();
    };

    const onSelectMember = (newUserUrn: string) => {
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
        const filteredUsers = searchResults
            .filter((result) => result.entity.urn === newUserUrn)
            .map((result) => result.entity);
        if (filteredUsers.length) {
            const newUser = filteredUsers[0] as CorpUser;
            const newUsers = [...(selectedUsers || []), newUser];
            setSelectedUsers(newUsers);
        }
    };

    const onDeselectMember = (userUrn: string) => {
        const newUserActors = selectedUsers.filter((user) => user.urn !== userUrn);
        setSelectedUsers(newUserActors);
    };

    const handleUserSearch = (text: string) => {
        if (text.length > 2) {
            userSearch({
                variables: {
                    input: {
                        type: EntityType.CorpUser,
                        query: text,
                        start: 0,
                        count: 5,
                    },
                },
            });
        }
    };

    // Renders a search result in the select dropdown.
    const renderSearchResult = (result: SearchResult) => {
        const avatarUrl = (result.entity as CorpUser).editableProperties?.pictureLink || undefined;
        const displayName = entityRegistry.getDisplayName(result.entity.type, result.entity);
        return (
            <SearchResultContainer>
                <SearchResultContent>
                    <CustomAvatar size={32} name={displayName} photoUrl={avatarUrl} isGroup={false} />
                    <SearchResultDisplayName>
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

    return (
        <Modal
            title="Add group members"
            visible={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button disabled={selectedUsers.length === 0} onClick={onAdd}>
                        Add
                    </Button>
                </>
            }
        >
            <Form component={false}>
                <Form.Item>
                    <Select
                        showSearch
                        autoFocus
                        ref={inputEl}
                        filterOption={false}
                        value={selectedUsers.map((user) => entityRegistry.getDisplayName(EntityType.CorpUser, user))}
                        mode="multiple"
                        placeholder="Search for users..."
                        onSelect={(actorUrn: any) => onSelectMember(actorUrn)}
                        onDeselect={(actorUrn: any) => onDeselectMember(actorUrn)}
                        onSearch={handleUserSearch}
                        tagRender={(tagProps) => <Tag>{tagProps.value}</Tag>}
                    >
                        {searchResults?.map((result) => (
                            <Select.Option value={result.entity.urn}>{renderSearchResult(result)}</Select.Option>
                        ))}
                    </Select>
                </Form.Item>
            </Form>
        </Modal>
    );
};
