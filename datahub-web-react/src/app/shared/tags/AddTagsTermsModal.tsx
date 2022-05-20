import React, { useState } from 'react';
import { message, Button, Modal, Select, Typography, Tag as CustomTag } from 'antd';
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
import { useAddTagsMutation, useAddTermsMutation } from '../../../graphql/mutations.generated';
import analytics, { EventType, EntityActionType } from '../../analytics';
import { useEnterKeyListener } from '../useEnterKeyListener';
import { StyledTag } from '../../entity/shared/components/styled/StyledTag';

type AddTagsModalProps = {
    globalTags?: GlobalTags | null;
    glossaryTerms?: GlossaryTerms | null;
    visible: boolean;
    onCloseModal: () => void;
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

const renderTerm = (suggestion: string, icon: JSX.Element, type: string) => ({
    value: suggestion,
    label: (
        <SuggestionContainer>
            <span>{icon}</span>
            <SuggestionText>{suggestion}</SuggestionText>
        </SuggestionContainer>
    ),
    type,
});

const renderTag = (suggestion: string, $colorHash, $color, type: string) => ({
    value: suggestion,
    label: (
        <StyledTag $colorHash={$colorHash} $color={$color} closable={false} style={{ border: 'none' }}>
            {suggestion}
        </StyledTag>
    ),
    type,
});

export default function AddTagsTermsModal({
    globalTags,
    glossaryTerms,
    visible,
    onCloseModal,
    entityUrn,
    entityType,
    entitySubresource,
    type = EntityType.Tag,
}: AddTagsModalProps) {
    const entityRegistry = useEntityRegistry();
    const [inputValue, setInputValue] = useState('');
    const [showCreateModal, setShowCreateModal] = useState(false);
    const [disableAdd, setDisableAdd] = useState(false);
    const [selectedValues, setSelectedValues] = useState<string[]>([]);

    const [addTagsMutation] = useAddTagsMutation();
    const [addTermsMutation] = useAddTermsMutation();

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
        const term = renderTerm(
            displayName,
            entityRegistry.getIcon(result.entity.type, 14, IconStyleType.ACCENT),
            result.entity.type,
        );

        const tag = renderTag(
            displayName,
            (result.entity as Tag).urn,
            (result.entity as Tag).properties?.colorHex,
            result.entity.type,
        );
        return result.entity.type === EntityType.Tag ? (
            <Select.Option value={`${tag.value}${NAME_TYPE_SEPARATOR}${tag.type}`} key={tag.value}>
                {tag.label}
            </Select.Option>
        ) : (
            <Select.Option value={`${term.value}${NAME_TYPE_SEPARATOR}${term.type}`} key={term.value}>
                {term.label}
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

    const tagRender = (props) => {
        // eslint-disable-next-line react/prop-types
        const { label, closable, onClose } = props;
        const onPreventMouseDown = (event) => {
            event.preventDefault();
            event.stopPropagation();
        };
        return (
            <CustomTag
                onMouseDown={onPreventMouseDown}
                closable={closable}
                onClose={onClose}
                style={{
                    marginRight: 3,
                    display: 'flex',
                    justifyContent: 'center',
                    alignItems: 'center',
                }}
            >
                {label}
            </CustomTag>
        );
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#addTagButton',
    });

    if (showCreateModal) {
        return (
            <CreateTagModal
                visible={visible}
                onClose={onCloseModal}
                onBack={() => setShowCreateModal(false)}
                tagName={inputValue}
                entityUrn={entityUrn}
                entitySubresource={entitySubresource}
            />
        );
    }

    // Function to handle the modal action's
    const onOk = () => {
        let mutation: ((input: any) => Promise<any>) | null = null;
        if (type === EntityType.Tag) {
            mutation = addTagsMutation;
        }
        if (type === EntityType.GlossaryTerm) {
            mutation = addTermsMutation;
        }

        if (!entityUrn || !mutation) {
            onCloseModal();
            return;
        }
        setDisableAdd(true);

        let input = {};
        let actionType = EntityActionType.UpdateSchemaTags;
        if (type === EntityType.Tag) {
            input = {
                tagUrns: selectedValues,
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
        if (type === EntityType.GlossaryTerm) {
            input = {
                termUrns: selectedValues,
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
                        content: `Added ${type === EntityType.GlossaryTerm ? 'Terms' : 'Tags'}!`,
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
                onCloseModal();
                setSelectedValues([]);
            });
    };

    const handleChange = (selectedInputValues) => {
        setSelectedValues([]);
        if (selectedInputValues.indexOf(CREATE_TAG_VALUE) >= 0) {
            setShowCreateModal(true);
            return;
        }
        // eslint-disable-next-line array-callback-return
        selectedInputValues.map((value) => {
            const { name: selectedName, type: selectedType } = getSelectedValue(value);
            let urnToAdd = '';
            if (selectedType === EntityType.Tag) {
                urnToAdd = `urn:li:tag:${selectedName}`;
            }
            if (selectedType === EntityType.GlossaryTerm) {
                urnToAdd = `urn:li:glossaryTerm:${selectedName}`;
            }
            setSelectedValues((prevState) => [...prevState, urnToAdd]);
        });
    };
    console.log('Values:: ', globalTags, glossaryTerms);

    return (
        <Modal
            title={`Add ${entityRegistry.getEntityName(type)}s`}
            visible={visible}
            onCancel={onCloseModal}
            okButtonProps={{ disabled: selectedValues.length === 0 }}
            okText="Add"
            footer={
                <>
                    <Button onClick={onCloseModal} type="text">
                        Cancel
                    </Button>
                    <Button
                        id="addTagButton"
                        data-testid="add-tag-term-from-modal-btn"
                        onClick={onOk}
                        disabled={selectedValues.length === 0 || disableAdd}
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
                mode="multiple"
                placeholder={`Search for ${entityRegistry.getEntityName(type)?.toLowerCase()}...`}
                defaultActiveFirstOption={false}
                showArrow={false}
                filterOption={false}
                onSearch={(value: string) => {
                    handleSearch(value.trim());
                    setInputValue(value.trim());
                }}
                onChange={handleChange}
                tagRender={tagRender}
            >
                {tagSearchOptions}
            </TagSelect>
        </Modal>
    );
}
