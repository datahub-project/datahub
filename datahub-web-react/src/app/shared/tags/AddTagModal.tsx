import React, { useState } from 'react';
import { FetchResult } from '@apollo/client';
import { Button, Modal, Select, Typography } from 'antd';
import styled from 'styled-components';

import { UpdateDatasetMutation } from '../../../graphql/dataset.generated';
import { useGetAutoCompleteResultsLazyQuery } from '../../../graphql/search.generated';
import { GlobalTags, GlobalTagsUpdate, EntityType, TagAssociationUpdate } from '../../../types.generated';
import { convertTagsForUpdate } from './utils/convertTagsForUpdate';
import CreateTagModal from './CreateTagModal';

type AddTagModalProps = {
    globalTags?: GlobalTags | null;
    updateTags?: (
        update: GlobalTagsUpdate,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>>>;
    visible: boolean;
    onClose: () => void;
};

const TagSelect = styled(Select)`
    width: 480px;
`;

const CREATE_TAG_VALUE = '____reserved____.createTagValue';

export default function AddTagModal({ updateTags, globalTags, visible, onClose }: AddTagModalProps) {
    const [getAutoCompleteResults, { loading, data: suggestionsData }] = useGetAutoCompleteResultsLazyQuery({
        fetchPolicy: 'no-cache',
    });
    const [inputValue, setInputValue] = useState('');
    const [selectedTagValue, setSelectedTagValue] = useState('');
    const [showCreateModal, setShowCreateModal] = useState(false);
    const [disableAdd, setDisableAdd] = useState(false);

    const autoComplete = (query: string) => {
        if (query && query !== '') {
            getAutoCompleteResults({
                variables: {
                    input: {
                        type: EntityType.Tag,
                        query,
                    },
                },
            });
        }
    };

    const inputExistsInAutocomplete = suggestionsData?.autoComplete?.suggestions?.some(
        (result) => result.toLowerCase() === inputValue.toLowerCase(),
    );

    const autocompleteOptions =
        suggestionsData?.autoComplete?.suggestions.map((result) => (
            <Select.Option value={result} key={result}>
                {result}
            </Select.Option>
        )) || [];

    if (!inputExistsInAutocomplete && inputValue.length > 0 && !loading) {
        autocompleteOptions.push(
            <Select.Option value={CREATE_TAG_VALUE} key={CREATE_TAG_VALUE}>
                <Typography.Link> Create {inputValue}</Typography.Link>
            </Select.Option>,
        );
    }

    const onOk = () => {
        if (!globalTags?.tags?.some((tag) => tag.tag.name === selectedTagValue)) {
            setDisableAdd(true);
            updateTags?.({
                tags: [
                    ...convertTagsForUpdate(globalTags?.tags || []),
                    { tag: { urn: `urn:li:tag:${selectedTagValue}`, name: selectedTagValue } },
                ] as TagAssociationUpdate[],
            }).finally(() => {
                setDisableAdd(false);
                onClose();
            });
        } else {
            onClose();
        }
    };

    if (showCreateModal) {
        return (
            <CreateTagModal
                updateTags={updateTags}
                globalTags={globalTags}
                visible={visible}
                onClose={onClose}
                onBack={() => setShowCreateModal(false)}
                tagName={inputValue}
            />
        );
    }

    return (
        <Modal
            title="Add tag"
            visible={visible}
            onCancel={onClose}
            okButtonProps={{ disabled: selectedTagValue.length === 0 }}
            okText="Add"
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button onClick={onOk} disabled={selectedTagValue.length === 0 || disableAdd}>
                        Add
                    </Button>
                </>
            }
        >
            <TagSelect
                allowClear
                autoFocus
                showSearch
                placeholder="Find a tag"
                defaultActiveFirstOption={false}
                showArrow={false}
                filterOption={false}
                onSearch={(value: string) => {
                    autoComplete(value);
                    setInputValue(value);
                }}
                onSelect={(selected) =>
                    selected === CREATE_TAG_VALUE ? setShowCreateModal(true) : setSelectedTagValue(String(selected))
                }
                notFoundContent={loading ? 'loading' : 'type to search'}
            >
                {autocompleteOptions}
            </TagSelect>
        </Modal>
    );
}
