import React, { useRef, useState } from 'react';
import { message, Modal, Button, Form, Select, Tag } from 'antd';
import styled from 'styled-components';
import { useAddGroupMembersMutation } from '../../../graphql/group.generated';
import { CorpUser, Entity, EntityType } from '../../../types.generated';
import { useGetSearchResultsLazyQuery } from '../../../graphql/search.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useGetRecommendations } from '../../shared/recommendation';
import { OwnerLabel } from '../../shared/OwnerLabel';

type Props = {
    urn: string;
    visible: boolean;
    onCloseModal: () => void;
    onSubmit: () => void;
};

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
    const [selectedMembers, setSelectedMembers] = useState<any[]>([]);
    const [inputValue, setInputValue] = useState('');
    const [addGroupMembersMutation] = useAddGroupMembersMutation();
    const [userSearch, { data: userSearchData }] = useGetSearchResultsLazyQuery();
    const searchResults = userSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];
    const [recommendedData] = useGetRecommendations([EntityType.CorpUser]);
    const inputEl = useRef(null);

    const handleUserSearch = (text: string) => {
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
    };

    // Renders a search result in the select dropdown.
    const renderSearchResult = (entity: Entity) => {
        const avatarUrl = (entity as CorpUser).editableProperties?.pictureLink || undefined;
        const displayName = entityRegistry.getDisplayName(entity.type, entity);
        return (
            <Select.Option value={entity.urn} key={entity.urn}>
                <OwnerLabel name={displayName} avatarUrl={avatarUrl} type={entity.type} />
            </Select.Option>
        );
    };

    const groupResult = !inputValue || inputValue.length === 0 ? recommendedData : searchResults;

    const groupSearchOptions = groupResult?.map((result) => {
        return renderSearchResult(result);
    });

    const onModalClose = () => {
        setInputValue('');
        setSelectedMembers([]);
        onCloseModal();
    };

    const onSelectMember = (newMemberUrn: string) => {
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
        const newUsers = [...(selectedMembers || []), newMemberUrn];
        setSelectedMembers(newUsers);
    };

    const onDeselectMember = (memberUrn: string) => {
        setInputValue('');
        const newUserActors = selectedMembers.filter((user) => user !== memberUrn);
        setSelectedMembers(newUserActors);
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
        const selectedMemberUrns = selectedMembers.map((selectedMember) => selectedMember.value);
        if (selectedMembers.length === 0) {
            return;
        }
        try {
            await addGroupMembersMutation({
                variables: {
                    groupUrn: urn,
                    userUrns: selectedMemberUrns,
                },
            });
            message.success({ content: 'Group members added!', duration: 3 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to group members: \n ${e.message || ''}`, duration: 3 });
            }
        } finally {
            onSubmit();
            onModalClose();
        }
    };

    function handleBlur() {
        setInputValue('');
    }

    return (
        <Modal
            title="Add group members"
            visible={visible}
            onCancel={onModalClose}
            footer={
                <>
                    <Button onClick={onModalClose} type="text">
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
                        labelInValue
                        autoFocus
                        defaultOpen
                        mode="multiple"
                        ref={inputEl}
                        placeholder="Search for users..."
                        showSearch
                        filterOption={false}
                        defaultActiveFirstOption={false}
                        onSelect={(actorUrn: any) => onSelectMember(actorUrn)}
                        onDeselect={(actorUrn: any) => onDeselectMember(actorUrn)}
                        onSearch={(value: string) => {
                            // eslint-disable-next-line react/prop-types
                            handleUserSearch(value.trim());
                            // eslint-disable-next-line react/prop-types
                            setInputValue(value.trim());
                        }}
                        tagRender={tagRender}
                        onBlur={handleBlur}
                        value={selectedMembers}
                    >
                        {groupSearchOptions}
                    </SelectInput>
                </Form.Item>
            </Form>
        </Modal>
    );
};
