import React, { useState } from 'react';
import { message, Button, Modal, Select, Typography } from 'antd';
import styled from 'styled-components';

import { useGetSearchResultsLazyQuery } from '../../../graphql/search.generated';
import {
    GlobalTags,
    EntityType,
    GlossaryTerms,
    SubResourceType,
    SearchResult,
    Tag,
    GlossaryTerm,
} from '../../../types.generated';
import CreateTagModal from './CreateTagModal';
import { useEntityRegistry } from '../../useEntityRegistry';
import { IconStyleType } from '../../entity/Entity';
import { useAddTagMutation, useAddTermMutation } from '../../../graphql/mutations.generated';
import analytics, { EventType, EntityActionType } from '../../analytics';
import { useEnterKeyListener } from '../useEnterKeyListener';

type AddTagModalProps = {
    globalTags?: GlobalTags | null;
    glossaryTerms?: GlossaryTerms | null;
    visible: boolean;
    onClose: () => void;
    entityUrn: string;
    entityType: EntityType;
    entitySubresource?: string;
    type?: EntityType;
};

const TagSelect = styled(Select)`
    width: 480px;
`;

const SuggestionContainer = styled.div`
    display: 'flex',
    flex-direction: 'row',
    align-items: 'center',
`;

const SuggestionText = styled.span`
    margin-left: 10px;
`;

const CREATE_TAG_VALUE = '____reserved____.createTagValue';

const NAME_TYPE_SEPARATOR = '_::_:_::_';

const getSelectedValue = (rawValue: string) => {
    const [name, type] = rawValue.split(NAME_TYPE_SEPARATOR);
    return {
        name,
        type,
    };
};

const renderItem = (suggestion: string, icon: JSX.Element, type: string) => ({
    value: suggestion,
    label: (
        <SuggestionContainer>
            <span>{icon}</span>
            <SuggestionText>{suggestion}</SuggestionText>
        </SuggestionContainer>
    ),
    type,
});

export default function AddTagTermModal({
    globalTags,
    glossaryTerms,
    visible,
    onClose,
    entityUrn,
    entityType,
    entitySubresource,
    type = EntityType.Tag,
}: AddTagModalProps) {
    const [inputValue, setInputValue] = useState('');
    const [selectedValue, setSelectedValue] = useState('');
    const [showCreateModal, setShowCreateModal] = useState(false);
    const [disableAdd, setDisableAdd] = useState(false);
    const entityRegistry = useEntityRegistry();
    const [addTagMutation] = useAddTagMutation();
    const [addTermMutation] = useAddTermMutation();
    const [tagTermSearch, { data: tagTermSearchData }] = useGetSearchResultsLazyQuery();
    const tagSearchResults = tagTermSearchData?.search?.searchResults || [];

    const handleSearch = (text: string) => {
        if (text.length > 0) {
            tagTermSearch({
                variables: {
                    input: {
                        type,
                        query: text,
                        start: 0,
                        count: 10,
                    },
                },
            });
        }
    };

    const renderSearchResult = (result: SearchResult) => {
        const displayName =
            result.entity.type === EntityType.Tag
                ? (result.entity as Tag).name
                : (result.entity as GlossaryTerm).hierarchicalName;
        const item = renderItem(
            displayName,
            entityRegistry.getIcon(result.entity.type, 14, IconStyleType.ACCENT),
            result.entity.type,
        );
        return (
            <Select.Option value={`${item.value}${NAME_TYPE_SEPARATOR}${item.type}`} key={item.value}>
                {item.label}
            </Select.Option>
        );
    };

    const tagSearchOptions = tagSearchResults.map((result) => {
        return renderSearchResult(result);
    });

    const inputExistsInTagSearch = tagSearchResults.some((result: SearchResult) => {
        const displayName = entityRegistry.getDisplayName(result.entity.type, result.entity);
        return displayName.toLowerCase() === inputValue.toLowerCase();
    });

    if (!inputExistsInTagSearch && inputValue.length > 0 && type === EntityType.Tag) {
        tagSearchOptions.push(
            <Select.Option value={CREATE_TAG_VALUE} key={CREATE_TAG_VALUE}>
                <Typography.Link> Create {inputValue}</Typography.Link>
            </Select.Option>,
        );
    }

    const onOk = () => {
        const { name: selectedName, type: selectedType } = getSelectedValue(selectedValue);
        let mutation: ((input: any) => Promise<any>) | null = null;

        if (selectedType === EntityType.Tag) {
            mutation = addTagMutation;
            if (globalTags?.tags?.some((tag) => tag.tag.name === selectedName)) {
                onClose();
                return;
            }
        }
        if (selectedType === EntityType.GlossaryTerm) {
            mutation = addTermMutation;
            if (glossaryTerms?.terms?.some((term) => term.term.hierarchicalName === selectedName)) {
                onClose();
                return;
            }
        }

        if (!entityUrn || !mutation) {
            onClose();
            return;
        }

        setDisableAdd(true);

        let urnToAdd = '';
        let input = {};
        let actionType = EntityActionType.UpdateSchemaTags;
        if (selectedType === EntityType.Tag) {
            urnToAdd = `urn:li:tag:${selectedName}`;
            input = {
                tagUrn: urnToAdd,
                resourceUrn: entityUrn,
                subResource: entitySubresource,
                subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
            };
            if (entitySubresource) {
                actionType = EntityActionType.UpdateSchemaTags;
            } else {
                actionType = EntityActionType.UpdateTags;
            }
        }
        if (selectedType === EntityType.GlossaryTerm) {
            urnToAdd = `urn:li:glossaryTerm:${selectedName}`;
            input = {
                termUrn: urnToAdd,
                resourceUrn: entityUrn,
                subResource: entitySubresource,
                subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
            };
            if (entitySubresource) {
                actionType = EntityActionType.UpdateSchemaTerms;
            } else {
                actionType = EntityActionType.UpdateTerms;
            }
        }

        analytics.event({
            type: EventType.EntityActionEvent,
            entityType,
            entityUrn,
            actionType,
        });

        mutation({
            variables: {
                input,
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content: `Added ${selectedType === EntityType.GlossaryTerm ? 'Term' : 'Tag'}!`,
                        duration: 2,
                    });
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to add: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                setDisableAdd(false);
                onClose();
            });
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#addTagButton',
    });

    if (showCreateModal) {
        return (
            <CreateTagModal
                visible={visible}
                onClose={onClose}
                onBack={() => setShowCreateModal(false)}
                tagName={inputValue}
                entityUrn={entityUrn}
                entitySubresource={entitySubresource}
            />
        );
    }

    return (
        <Modal
            title={`Add ${entityRegistry.getEntityName(type)}`}
            visible={visible}
            onCancel={onClose}
            okButtonProps={{ disabled: selectedValue.length === 0 }}
            okText="Add"
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button
                        id="addTagButton"
                        data-testid="add-tag-term-from-modal-btn"
                        onClick={onOk}
                        disabled={selectedValue.length === 0 || disableAdd}
                    >
                        Add
                    </Button>
                </>
            }
        >
            <TagSelect
                allowClear
                autoFocus
                showSearch
                placeholder={`Search for ${entityRegistry.getEntityName(type)?.toLowerCase()}...`}
                defaultActiveFirstOption={false}
                showArrow={false}
                filterOption={false}
                onSearch={(value: string) => {
                    handleSearch(value.trim());
                    setInputValue(value.trim());
                }}
                onSelect={(selected) =>
                    selected === CREATE_TAG_VALUE ? setShowCreateModal(true) : setSelectedValue(String(selected))
                }
            >
                {tagSearchOptions}
            </TagSelect>
        </Modal>
    );
}
