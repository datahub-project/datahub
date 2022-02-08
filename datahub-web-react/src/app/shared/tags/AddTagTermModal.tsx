import React, { useState } from 'react';
import { message, Button, Modal, Select, Typography } from 'antd';
import styled from 'styled-components';

import { useGetAutoCompleteMultipleResultsLazyQuery } from '../../../graphql/search.generated';
import {
    GlobalTags,
    EntityType,
    AutoCompleteResultForEntity,
    GlossaryTerms,
    SubResourceType,
} from '../../../types.generated';
import CreateTagModal from './CreateTagModal';
import { useEntityRegistry } from '../../useEntityRegistry';
import { IconStyleType } from '../../entity/Entity';
import { useAddTagMutation, useAddTermMutation } from '../../../graphql/mutations.generated';
import { useProposeTagMutation, useProposeTermMutation } from '../../../graphql/proposals.generated';
import analytics, { EventType, EntityActionType } from '../../analytics';

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

const AddFooter = styled.div`
    display: flex;
    justify-content: space-between;
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
    const [getAutoCompleteResults, { loading, data: suggestionsData }] = useGetAutoCompleteMultipleResultsLazyQuery({
        fetchPolicy: 'no-cache',
    });
    const [inputValue, setInputValue] = useState('');
    const [selectedValue, setSelectedValue] = useState('');
    const [showCreateModal, setShowCreateModal] = useState(false);
    const [disableAdd, setDisableAdd] = useState(false);
    const entityRegistry = useEntityRegistry();
    const [addTagMutation] = useAddTagMutation();
    const [addTermMutation] = useAddTermMutation();
    const [proposeTagMutation] = useProposeTagMutation();
    const [proposeTermMutation] = useProposeTermMutation();

    const autoComplete = (query: string) => {
        if (query && query !== '') {
            getAutoCompleteResults({
                variables: {
                    input: {
                        types: [type],
                        query,
                        limit: 25,
                    },
                },
            });
        }
    };

    const options =
        suggestionsData?.autoCompleteForMultiple?.suggestions.flatMap((entity: AutoCompleteResultForEntity) =>
            entity.suggestions.map((suggestion: string) =>
                renderItem(suggestion, entityRegistry.getIcon(entity.type, 14, IconStyleType.TAB_VIEW), entity.type),
            ),
        ) || [];

    const inputExistsInAutocomplete = options.some((option) => option.value.toLowerCase() === inputValue.toLowerCase());

    const autocompleteOptions =
        options.map((option) => (
            <Select.Option value={`${option.value}${NAME_TYPE_SEPARATOR}${option.type}`} key={option.value}>
                {option.label}
            </Select.Option>
        )) || [];

    if (!inputExistsInAutocomplete && inputValue.length > 0 && !loading && type === EntityType.Tag) {
        autocompleteOptions.push(
            <Select.Option value={CREATE_TAG_VALUE} key={CREATE_TAG_VALUE}>
                <Typography.Link> Create {inputValue}</Typography.Link>
            </Select.Option>,
        );
    }

    const onOk = (isProposal: boolean) => {
        const { name: selectedName, type: selectedType } = getSelectedValue(selectedValue);
        let mutation: ((input: any) => Promise<any>) | null = null;

        if (selectedType === EntityType.Tag) {
            mutation = isProposal ? proposeTagMutation : addTagMutation;
            if (globalTags?.tags?.some((tag) => tag.tag.name === selectedName)) {
                onClose();
                return;
            }
        }
        if (selectedType === EntityType.GlossaryTerm) {
            mutation = isProposal ? proposeTermMutation : addTermMutation;
            if (glossaryTerms?.terms?.some((term) => term.term.name === selectedName)) {
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
                        content: `${isProposal ? 'Proposed' : 'Added'} ${
                            selectedType === EntityType.GlossaryTerm ? 'Term' : 'Tag'
                        }!`,
                        duration: 2,
                    });
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: `Failed to ${isProposal ? 'propose' : 'add'}: \n ${e.message || ''}`,
                    duration: 3,
                });
            })
            .finally(() => {
                setDisableAdd(false);
                onClose();
            });
    };

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
                <AddFooter>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <div>
                        <Button
                            onClick={() => onOk(true)}
                            disabled={selectedValue.length === 0 || disableAdd}
                            data-testid="create-proposal-btn"
                        >
                            Propose
                        </Button>
                        <Button
                            type="primary"
                            onClick={() => onOk(false)}
                            disabled={selectedValue.length === 0 || disableAdd}
                            data-testid="add-tag-term-from-modal-btn"
                        >
                            Add
                        </Button>
                    </div>
                </AddFooter>
            }
        >
            <TagSelect
                allowClear
                autoFocus
                showSearch
                placeholder={`Find a ${entityRegistry.getEntityName(type)?.toLowerCase()}`}
                defaultActiveFirstOption={false}
                showArrow={false}
                filterOption={false}
                onSearch={(value: string) => {
                    autoComplete(value.trim());
                    setInputValue(value.trim());
                }}
                onSelect={(selected) =>
                    selected === CREATE_TAG_VALUE ? setShowCreateModal(true) : setSelectedValue(String(selected))
                }
                notFoundContent={loading ? 'loading' : 'type to search'}
            >
                {autocompleteOptions}
            </TagSelect>
        </Modal>
    );
}
