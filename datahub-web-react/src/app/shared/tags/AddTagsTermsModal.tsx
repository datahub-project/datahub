import React, { useState } from 'react';
import { message, Button, Modal, Select, Typography, Tag as CustomTag } from 'antd';
import styled from 'styled-components';
import { BookOutlined } from '@ant-design/icons';

import { useGetSearchResultsLazyQuery } from '../../../graphql/search.generated';
import { EntityType, SubResourceType, SearchResult, Tag, GlossaryTerm } from '../../../types.generated';
import CreateTagModal from './CreateTagModal';
import { useEntityRegistry } from '../../useEntityRegistry';
import { IconStyleType } from '../../entity/Entity';
import { useAddTagsMutation, useAddTermsMutation } from '../../../graphql/mutations.generated';
import analytics, { EventType, EntityActionType } from '../../analytics';
import { useEnterKeyListener } from '../useEnterKeyListener';
import { StyledTag } from '../../entity/shared/components/styled/StyledTag';

type AddTagsModalProps = {
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
    margin-left: 2px;
    font-size: 10px;
    line-height: 20px;
    white-space: nowrap;
    margin-right: 8px;
    opacity: 1;
    color: #434343;
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
            <BookOutlined style={{ marginRight: '3%' }} />
            <SuggestionText>{suggestion}</SuggestionText>
        </SuggestionContainer>
    ),
    type,
});

const renderTag = (suggestion: string, $colorHash, $color, type: string) => ({
    value: suggestion,
    label: (
        <StyledTag
            $colorHash={$colorHash}
            $color={$color}
            closable={false}
            style={{
                border: 'none',
                marginLeft: '0px',
                fontSize: '10px',
                lineHeight: '20px',
                whiteSpace: 'nowrap',
                marginRight: '0px',
                opacity: 1,
                color: '#434343',
            }}
        >
            {suggestion}
        </StyledTag>
    ),
    type,
});

export default function AddTagsTermsModal({
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
    const [urnIds, setUrnIds] = useState<string[]>([]);

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
        const { label, value, closable, onClose } = props;
        const { type: selectedType } = getSelectedValue(value);
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
                    padding: selectedType === EntityType.Tag ? '0px 7px 0px 0px' : '0px 7px',
                    marginRight: 3,
                    borderRadius: '100em',
                    lineHeight: 0,
                    display: 'flex',
                    justifyContent: 'start',
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

    const getUrnId = (urnId: string) => {
        const { name: selectedName, type: selectedType } = getSelectedValue(urnId);
        let tempUrn = '';
        if (selectedType === EntityType.Tag) {
            tempUrn = `urn:li:tag:${selectedName}`;
        }
        if (selectedType === EntityType.GlossaryTerm) {
            tempUrn = `urn:li:glossaryTerm:${selectedName}`;
        }
        return tempUrn;
    };

    // When a Tag or term search result is selected, add the urn to the UrnIds
    const onSelectValue = (newUrnId: string) => {
        if (newUrnId === CREATE_TAG_VALUE) {
            setShowCreateModal(true);
            return;
        }
        const newUrnIds = [...(urnIds || []), getUrnId(newUrnId)];
        setUrnIds(newUrnIds);
    };

    // When a Tag or term search result is deselected, remove the urn from the Owners
    const onDeselectValue = (urnId: string) => {
        const newUrnIds = urnIds?.filter((u) => u !== getUrnId(urnId));
        setUrnIds(newUrnIds);
    };

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
                tagUrns: urnIds,
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
                termUrns: urnIds,
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
                setUrnIds([]);
            });
    };

    return (
        <Modal
            title={`Add ${entityRegistry.getEntityName(type)}s`}
            visible={visible}
            onCancel={onCloseModal}
            okButtonProps={{ disabled: urnIds.length === 0 }}
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
                        disabled={urnIds.length === 0 || disableAdd}
                    >
                        Add
                    </Button>
                </>
            }
        >
            <TagSelect
                autoFocus
                mode="multiple"
                filterOption={false}
                placeholder={`Search for ${entityRegistry.getEntityName(type)?.toLowerCase()}...`}
                showSearch
                defaultActiveFirstOption={false}
                onSelect={(asset: any) => onSelectValue(asset)}
                onDeselect={(asset: any) => onDeselectValue(asset)}
                onSearch={(value: string) => {
                    // eslint-disable-next-line react/prop-types
                    handleSearch(value.trim());
                    // eslint-disable-next-line react/prop-types
                    setInputValue(value.trim());
                }}
                tagRender={tagRender}
            >
                {tagSearchOptions}
            </TagSelect>
        </Modal>
    );
}
