import React, { useRef, useState } from 'react';
import { message, Button, Modal, Select, Typography, Tag as CustomTag } from 'antd';
import styled from 'styled-components';

import { useGetSearchResultsLazyQuery } from '../../../graphql/search.generated';
import { EntityType, Tag, Entity, ResourceRefInput } from '../../../types.generated';
import CreateTagModal from './CreateTagModal';
import {
    useBatchAddTagsMutation,
    useBatchAddTermsMutation,
    useBatchRemoveTagsMutation,
    useBatchRemoveTermsMutation,
} from '../../../graphql/mutations.generated';
import { useEnterKeyListener } from '../useEnterKeyListener';
import GlossaryBrowser from '../../glossary/GlossaryBrowser/GlossaryBrowser';
import ClickOutside from '../ClickOutside';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useGetRecommendations } from '../recommendation';
import { FORBIDDEN_URN_CHARS_REGEX } from '../../entity/shared/utils';
import { TagTermLabel } from './TagTermLabel';

export enum OperationType {
    ADD,
    REMOVE,
}

type EditTagsModalProps = {
    visible: boolean;
    onCloseModal: () => void;
    resources: ResourceRefInput[];
    type?: EntityType;
    operationType?: OperationType;
    defaultValues?: { urn: string; entity?: Entity | null }[];
    onOkOverride?: (result: string[]) => void;
};

const TagSelect = styled(Select)`
    width: 480px;
`;

const StyleTag = styled(CustomTag)`
    margin: 2px;
    display: flex;
    justify-content: start;
    align-items: center;
    white-space: nowrap;
    opacity: 1;
    color: #434343;
    line-height: 16px;
`;

export const BrowserWrapper = styled.div<{ isHidden: boolean }>`
    background-color: white;
    border-radius: 5px;
    box-shadow: 0 3px 6px -4px rgb(0 0 0 / 12%), 0 6px 16px 0 rgb(0 0 0 / 8%), 0 9px 28px 8px rgb(0 0 0 / 5%);
    max-height: 380px;
    overflow: auto;
    position: absolute;
    transition: opacity 0.2s;
    width: 480px;
    z-index: 1051;
    ${(props) =>
        props.isHidden &&
        `
        opacity: 0;
        height: 0;
    `}
`;

const CREATE_TAG_VALUE = '____reserved____.createTagValue';

const isValidTagName = (tagName: string) => {
    return tagName && tagName.length > 0 && !FORBIDDEN_URN_CHARS_REGEX.test(tagName);
};

const defaultValuesToSelectedValue = (defaultValues?: { urn: string; entity?: Entity | null }[]): any[] => {
    return (
        defaultValues?.map((defaultValue) => ({
            urn: defaultValue.urn,
            component: <TagTermLabel entity={defaultValue.entity} />,
        })) || []
    );
};

export default function EditTagTermsModal({
    visible,
    onCloseModal,
    resources,
    type = EntityType.Tag,
    operationType = OperationType.ADD,
    defaultValues = [],
    onOkOverride,
}: EditTagsModalProps) {
    const entityRegistry = useEntityRegistry();
    const [inputValue, setInputValue] = useState('');
    const [showCreateModal, setShowCreateModal] = useState(false);
    const [disableAction, setDisableAction] = useState(false);
    const [urns, setUrns] = useState<string[]>(defaultValues.map((defaultValue) => defaultValue.urn));
    const [selectedTerms, setSelectedTerms] = useState<any[]>(
        type === EntityType.GlossaryTerm ? defaultValuesToSelectedValue(defaultValues) : [],
    );

    const [selectedTags, setSelectedTags] = useState<any[]>(
        type === EntityType.Tag ? defaultValuesToSelectedValue(defaultValues) : [],
    );

    const [isFocusedOnInput, setIsFocusedOnInput] = useState(false);

    const [batchAddTagsMutation] = useBatchAddTagsMutation();
    const [batchRemoveTagsMutation] = useBatchRemoveTagsMutation();
    const [batchAddTermsMutation] = useBatchAddTermsMutation();
    const [batchRemoveTermsMutation] = useBatchRemoveTermsMutation();

    const [tagTermSearch, { data: tagTermSearchData }] = useGetSearchResultsLazyQuery();
    const tagSearchResults = tagTermSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];
    const [recommendedData] = useGetRecommendations([EntityType.Tag]);
    const inputEl = useRef(null);

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

    const renderSearchResult = (entity: Entity) => {
        const displayName =
            entity.type === EntityType.Tag ? (entity as Tag).name : entityRegistry.getDisplayName(entity.type, entity);
        const tagOrTermComponent = <TagTermLabel entity={entity} />;
        return (
            <Select.Option data-testid="tag-term-option" value={entity.urn} key={entity.urn} name={displayName}>
                {tagOrTermComponent}
            </Select.Option>
        );
    };

    const tagResult =
        (!inputValue || inputValue.length === 0) && type === EntityType.Tag ? recommendedData : tagSearchResults;

    const tagSearchOptions = tagResult?.map((result) => {
        return renderSearchResult(result);
    });

    const inputExistsInTagSearch = tagSearchResults.some((entity: Entity) => {
        const displayName = entityRegistry.getDisplayName(entity.type, entity);
        return displayName.toLowerCase() === inputValue.toLowerCase();
    });

    if (
        operationType === OperationType.ADD &&
        !inputExistsInTagSearch &&
        isValidTagName(inputValue) &&
        type === EntityType.Tag &&
        urns.length === 0
    ) {
        tagSearchOptions?.push(
            <Select.Option value={CREATE_TAG_VALUE} key={CREATE_TAG_VALUE}>
                <Typography.Link> Create {inputValue}</Typography.Link>
            </Select.Option>,
        );
    }

    const tagRender = (props) => {
        // eslint-disable-next-line react/prop-types
        const { closable, onClose, value } = props;
        const onPreventMouseDown = (event) => {
            event.preventDefault();
            event.stopPropagation();
        };
        const selectedItem =
            type === EntityType.GlossaryTerm
                ? selectedTerms.find((term) => term.urn === value).component
                : selectedTags.find((term) => term.urn === value).component;

        return (
            <StyleTag onMouseDown={onPreventMouseDown} closable={closable} onClose={onClose}>
                {selectedItem}
            </StyleTag>
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
                resources={resources}
            />
        );
    }

    // When a Tag or term search result is selected, add the urn to the Urns
    const onSelectValue = (urn: string) => {
        if (urn === CREATE_TAG_VALUE) {
            if (isValidTagName(inputValue)) {
                setShowCreateModal(true);
            }
            return;
        }
        const newUrns = [...(urns || []), urn];
        const selectedSearchOption = tagSearchOptions?.find((option) => option.props.value === urn);
        const selectedTagOption = tagResult?.find((tag) => tag.urn === urn);
        setUrns(newUrns);
        setSelectedTerms([
            ...selectedTerms,
            { urn, component: <TagTermLabel termName={selectedSearchOption?.props.name} /> },
        ]);
        setSelectedTags([
            ...selectedTags,
            {
                urn,
                component: <TagTermLabel entity={selectedTagOption} />,
            },
        ]);
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
    };

    // When a Tag or term search result is deselected, remove the urn from the Owners
    const onDeselectValue = (urn: string) => {
        const newUrns = urns?.filter((u) => u !== urn);
        setUrns(newUrns);
        setInputValue('');
        setIsFocusedOnInput(true);
        setSelectedTerms(selectedTerms.filter((term) => term.urn !== urn));
        setSelectedTags(selectedTags.filter((term) => term.urn !== urn));
    };

    const batchAddTags = () => {
        batchAddTagsMutation({
            variables: {
                input: {
                    tagUrns: urns,
                    resources,
                },
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
                setDisableAction(false);
                onCloseModal();
                setUrns([]);
            });
    };

    const batchAddTerms = () => {
        batchAddTermsMutation({
            variables: {
                input: {
                    termUrns: urns,
                    resources,
                },
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
                setDisableAction(false);
                onCloseModal();
                setUrns([]);
            });
    };

    const batchRemoveTags = () => {
        batchRemoveTagsMutation({
            variables: {
                input: {
                    tagUrns: urns,
                    resources,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content: `Removed ${type === EntityType.GlossaryTerm ? 'Terms' : 'Tags'}!`,
                        duration: 2,
                    });
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to remove: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                setDisableAction(false);
                onCloseModal();
                setUrns([]);
            });
    };

    const batchRemoveTerms = () => {
        batchRemoveTermsMutation({
            variables: {
                input: {
                    termUrns: urns,
                    resources,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content: `Removed ${type === EntityType.GlossaryTerm ? 'Terms' : 'Tags'}!`,
                        duration: 2,
                    });
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to remove: \n ${e.message || ''}`, duration: 3 });
            })
            .finally(() => {
                setDisableAction(false);
                onCloseModal();
                setUrns([]);
            });
    };

    const editTags = () => {
        if (operationType === OperationType.ADD) {
            batchAddTags();
        } else {
            batchRemoveTags();
        }
    };

    const editTerms = () => {
        if (operationType === OperationType.ADD) {
            batchAddTerms();
        } else {
            batchRemoveTerms();
        }
    };

    // Function to handle the modal action's
    const onOk = () => {
        if (onOkOverride) {
            onOkOverride(urns);
            return;
        }

        if (!resources) {
            onCloseModal();
            return;
        }
        setDisableAction(true);

        if (type === EntityType.Tag) {
            editTags();
        } else {
            editTerms();
        }
    };

    function selectTermFromBrowser(urn: string, displayName: string) {
        setIsFocusedOnInput(false);
        const newUrns = [...(urns || []), urn];
        setUrns(newUrns);
        setSelectedTerms([...selectedTerms, { urn, component: <TagTermLabel termName={displayName} /> }]);
    }

    function clearInput() {
        setInputValue('');
        setTimeout(() => setIsFocusedOnInput(true), 0); // call after click outside
    }

    function handleBlur() {
        setInputValue('');
    }

    const isShowingGlossaryBrowser = !inputValue && type === EntityType.GlossaryTerm && isFocusedOnInput;

    return (
        <Modal
            title={`${operationType === OperationType.ADD ? 'Add' : 'Remove'} ${entityRegistry.getEntityName(type)}s`}
            visible={visible}
            onCancel={onCloseModal}
            footer={
                <>
                    <Button onClick={onCloseModal} type="text">
                        Cancel
                    </Button>
                    <Button
                        id="addTagButton"
                        data-testid="add-tag-term-from-modal-btn"
                        onClick={onOk}
                        disabled={urns.length === 0 || disableAction}
                    >
                        Done
                    </Button>
                </>
            }
        >
            <ClickOutside onClickOutside={() => setIsFocusedOnInput(false)}>
                <TagSelect
                    data-testid="tag-term-modal-input"
                    autoFocus
                    defaultOpen
                    mode="multiple"
                    ref={inputEl}
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
                    value={urns}
                    onClear={clearInput}
                    onFocus={() => setIsFocusedOnInput(true)}
                    onBlur={handleBlur}
                    dropdownStyle={isShowingGlossaryBrowser ? { display: 'none' } : {}}
                >
                    {tagSearchOptions}
                </TagSelect>
                <BrowserWrapper isHidden={!isShowingGlossaryBrowser}>
                    <GlossaryBrowser isSelecting selectTerm={selectTermFromBrowser} />
                </BrowserWrapper>
            </ClickOutside>
        </Modal>
    );
}
