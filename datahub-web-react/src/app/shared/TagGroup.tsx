import { Modal, Tag, Space, Select, Input, Button, Typography } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { FetchResult } from '@apollo/client';

import { useEntityRegistry } from '../useEntityRegistry';
import { EntityType, GlobalTags, GlobalTagsUpdate, TagAssociation, TagAssociationUpdate } from '../../types.generated';
import { useGetAutoCompleteResultsLazyQuery } from '../../graphql/search.generated';
import { useUpdateTagMutation } from '../../graphql/tag.generated';
import { UpdateDatasetMutation } from '../../graphql/dataset.generated';

type Props = {
    uneditableTags?: GlobalTags | null;
    editableTags?: GlobalTags | null;
    canRemove?: boolean;
    canAdd?: boolean;
    updateTags?: (
        update: GlobalTagsUpdate,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>>>;
    onOpenModal?: () => void;
    maxShow?: number;
};

type AddTagModalProps = {
    globalTags?: GlobalTags | null;
    updateTags?: (
        update: GlobalTagsUpdate,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>>>;
    visible: boolean;
    onClose: () => void;
};

type CreateTagModalProps = {
    globalTags?: GlobalTags | null;
    updateTags?: (
        update: GlobalTagsUpdate,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>>>;
    visible: boolean;
    onClose: () => void;
    onBack: () => void;
    tagName: string;
};

export function convertTagsForUpdate(tags: TagAssociation[]): TagAssociationUpdate[] {
    return tags.map((tag) => ({
        tag: { urn: tag.tag.urn, name: tag.tag.name, description: tag.tag.description, type: EntityType.Tag },
    }));
}

const AddNewTag = styled(Tag)`
    cursor: pointer;
`;

const TagSelect = styled(Select)`
    width: 480px;
`;

const FullWidthSpace = styled(Space)`
    width: 100%;
`;

const CREATE_TAG_VALUE = '____reserved____.createTagValue';

function CreateTagModal({ updateTags, globalTags, onClose, onBack, visible, tagName }: CreateTagModalProps) {
    const [stagedDescription, setStagedDescription] = useState('');

    const [updateTagMutation, updateTagStatus] = useUpdateTagMutation();
    console.log({ updateTags, globalTags, updateTagStatus });

    const onOk = () => {
        // first create the new tag
        updateTagMutation({
            variables: {
                input: {
                    type: EntityType.Tag,
                    urn: `urn:li:tag:${tagName}`,
                    name: tagName,
                    description: stagedDescription,
                },
            },
        }).then(() => {
            // then apply the tag to the dataset
            updateTags?.({
                tags: [
                    ...convertTagsForUpdate(globalTags?.tags || []),
                    { tag: { urn: `urn:li:tag:${tagName}`, type: EntityType.Tag, name: tagName } },
                ] as TagAssociationUpdate[],
            }).then(onClose);
        });
    };

    return (
        <Modal
            title={`Create ${tagName}`}
            visible={visible}
            footer={
                <>
                    <Button onClick={onBack}>Back</Button>
                    <Button onClick={onOk} disabled={stagedDescription.length === 0 || updateTagStatus.loading}>
                        Create
                    </Button>
                </>
            }
        >
            <FullWidthSpace direction="vertical">
                <Input.TextArea
                    placeholder="Write a description for your new tag..."
                    value={stagedDescription}
                    onChange={(e) => setStagedDescription(e.target.value)}
                />
            </FullWidthSpace>
        </Modal>
    );
}

function AddTagModal({ updateTags, globalTags, visible, onClose }: AddTagModalProps) {
    const [getAutoCompleteResults, { loading, data: suggestionsData }] = useGetAutoCompleteResultsLazyQuery();
    const [selectInputValue, setSelectInputValue] = useState('');
    const [selectedTagValue, setSetSelectTagValue] = useState('');
    const [showCreateModal, setShowCreateModal] = useState(false);

    const autoComplete = (query: string) => {
        getAutoCompleteResults({
            variables: {
                input: {
                    type: EntityType.Tag,
                    query,
                },
            },
        });
    };

    const hasExactMatch = suggestionsData?.autoComplete?.suggestions?.some((result) => result === selectInputValue);

    const options =
        suggestionsData?.autoComplete?.suggestions.map((result) => (
            <Select.Option value={result} key={result}>
                {result}
            </Select.Option>
        )) || [];

    if (!hasExactMatch && selectInputValue.length > 2 && !loading) {
        options.push(
            <Select.Option value={CREATE_TAG_VALUE} key={CREATE_TAG_VALUE}>
                <Typography.Link> Create {selectInputValue}</Typography.Link>
            </Select.Option>,
        );
    }

    if (showCreateModal) {
        return (
            <CreateTagModal
                updateTags={updateTags}
                globalTags={globalTags}
                visible={visible}
                onClose={onClose}
                onBack={() => setShowCreateModal(false)}
                tagName={selectInputValue}
            />
        );
    }

    const onOk = () => {
        updateTags?.({
            tags: [
                ...convertTagsForUpdate(globalTags?.tags || []),
                { tag: { urn: `urn:li:tag:${selectedTagValue}`, type: EntityType.Tag, name: selectedTagValue } },
            ] as TagAssociationUpdate[],
        });
        onClose();
    };

    return (
        <Modal
            title="Add tag"
            visible={visible}
            onCancel={onClose}
            okButtonProps={{ disabled: selectedTagValue.length === 0 }}
            okText="Add"
            footer={
                <>
                    <Button onClick={onClose}>Cancel</Button>
                    <Button onClick={onOk} disabled={selectedTagValue.length === 0}>
                        Add
                    </Button>
                </>
            }
        >
            <TagSelect
                allowClear
                showSearch
                placeholder="Find a tag"
                defaultActiveFirstOption={false}
                showArrow={false}
                filterOption={false}
                onSearch={(value: string) => {
                    autoComplete(value);
                    setSelectInputValue(value);
                }}
                onSelect={(selected) =>
                    selected === CREATE_TAG_VALUE ? setShowCreateModal(true) : setSetSelectTagValue(String(selected))
                }
                notFoundContent={loading ? 'loading' : 'type at least 3 character to search'}
            >
                {options}
            </TagSelect>
        </Modal>
    );
}

export default function TagGroup({
    uneditableTags,
    editableTags,
    canRemove,
    canAdd,
    updateTags,
    onOpenModal,
    maxShow,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const [showAddModal, setShowAddModal] = useState(false);

    const removeTag = (urnToRemove: string) => {
        onOpenModal?.();
        const tagToRemove = editableTags?.tags?.find((tag) => tag.tag.urn === urnToRemove);
        const newTags = editableTags?.tags?.filter((tag) => tag.tag.urn !== urnToRemove);
        Modal.confirm({
            title: `Do you want to remove ${tagToRemove?.tag.name} tag?`,
            content: `Are you sure you want to remove the ${tagToRemove?.tag.name} tag?`,
            onOk() {
                updateTags?.({ tags: convertTagsForUpdate(newTags || []) });
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    let renderedTags = 0;

    return (
        <div>
            {uneditableTags?.tags?.map((tag) => {
                renderedTags += 1;
                if (maxShow && renderedTags > maxShow) return null;
                return (
                    <Link to={`/${entityRegistry.getPathName(EntityType.Tag)}/${tag.tag.urn}`} key={tag.tag.urn}>
                        <Tag color="blue" closable={false}>
                            {tag.tag.name}
                        </Tag>
                    </Link>
                );
            })}
            {editableTags?.tags?.map((tag) => {
                renderedTags += 1;
                if (maxShow && renderedTags > maxShow) return null;
                return (
                    <Link to={`/${entityRegistry.getPathName(EntityType.Tag)}/${tag.tag.urn}`} key={tag.tag.urn}>
                        <Tag
                            color="blue"
                            closable={canRemove}
                            onClose={(e) => {
                                e.preventDefault();
                                removeTag(tag.tag.urn);
                            }}
                        >
                            {tag.tag.name}
                        </Tag>
                    </Link>
                );
            })}
            {canAdd && (
                <>
                    <AddNewTag color="success" onClick={() => setShowAddModal(true)}>
                        + Add Tag
                    </AddNewTag>
                    {showAddModal && (
                        <AddTagModal
                            globalTags={editableTags}
                            updateTags={updateTags}
                            visible={showAddModal}
                            onClose={() => {
                                onOpenModal?.();
                                setShowAddModal(false);
                            }}
                        />
                    )}
                </>
            )}
        </div>
    );
}
