import React, { useState } from 'react';
import { message, Modal, Button, Form, Select, Tag } from 'antd';
import styled from 'styled-components';
import { useAddGroupMembersMutation } from '../../../graphql/group.generated';
import { CorpUser, EntityType, SearchResult } from '../../../types.generated';
import { CustomAvatar } from '../../shared/avatar';
import { useGetSearchResultsLazyQuery } from '../../../graphql/search.generated';
import { useEntityRegistry } from '../../useEntityRegistry';

type Props = {
    urn: string;
    visible: boolean;
    onCloseModal: () => void;
    onSubmit: () => void;
};

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

const StyleTag = styled(Tag)`
    padding: 0px 7px 0px 0px;
    margin-right: 3px;
    display: flex;
    justify-content: start;
    align-items: center;
`;

export const AddGroupMembersModal = ({ urn, visible, onCloseModal, onSubmit }: Props) => {
    const entityRegistry = useEntityRegistry();
    const [selectedMembers, setSelectedMembers] = useState<string[]>([]);
    const [addGroupMembersMutation] = useAddGroupMembersMutation();
    const [userSearch, { data: userSearchData }] = useGetSearchResultsLazyQuery();
    const searchResults = userSearchData?.search?.searchResults || [];

    const onSelectMember = (newMemberUrn: string) => {
        const newUsers = [...(selectedMembers || []), newMemberUrn];
        setSelectedMembers(newUsers);
    };

    const onDeselectMember = (memberUrn: string) => {
        const newUserActors = selectedMembers.filter((user) => user !== memberUrn);
        setSelectedMembers(newUserActors);
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
                    <CustomAvatar size={24} name={displayName} photoUrl={avatarUrl} isGroup={false} />
                    <div>{displayName}</div>
                </SearchResultContent>
            </SearchResultContainer>
        );
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

    const onAdd = async () => {
        if (selectedMembers.length === 0) {
            return;
        }
        addGroupMembersMutation({
            variables: {
                groupUrn: urn,
                userUrns: selectedMembers,
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
                setSelectedMembers([]);
            });
        onCloseModal();
    };

    return (
        <Modal
            title="Add group members"
            visible={visible}
            onCancel={onCloseModal}
            footer={
                <>
                    <Button onClick={onCloseModal} type="text">
                        Cancel
                    </Button>
                    <Button disabled={selectedMembers.length === 0} onClick={onAdd}>
                        Add
                    </Button>
                </>
            }
        >
            <Form component={false}>
                <Form.Item>
                    <SelectInput
                        showSearch
                        value={selectedMembers}
                        autoFocus
                        mode="multiple"
                        filterOption={false}
                        placeholder="Search for users..."
                        onSelect={(actorUrn: any) => onSelectMember(actorUrn)}
                        onDeselect={(actorUrn: any) => onDeselectMember(actorUrn)}
                        onSearch={handleUserSearch}
                        tagRender={tagRender}
                    >
                        {searchResults?.map((result) => (
                            <Select.Option value={result.entity.urn}>{renderSearchResult(result)}</Select.Option>
                        ))}
                    </SelectInput>
                </Form.Item>
            </Form>
        </Modal>
    );
};
