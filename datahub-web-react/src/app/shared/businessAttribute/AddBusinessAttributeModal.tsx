import React, { useRef, useState } from 'react';
import { Button, message, Modal, Select, Tag as CustomTag } from 'antd';
import styled from 'styled-components';
import { GlobalOutlined } from '@ant-design/icons';
import { Entity, EntityType, ResourceRefInput } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { handleBatchError } from '../../entity/shared/utils';
import {
    useAddBusinessAttributeMutation,
    useRemoveBusinessAttributeMutation,
} from '../../../graphql/mutations.generated';
import { useGetSearchResultsLazyQuery } from '../../../graphql/search.generated';
import ClickOutside from '../ClickOutside';
import { useGetRecommendations } from '../recommendation';
import { useEnterKeyListener } from '../useEnterKeyListener';
import { ENTER_KEY_CODE } from '../constants';
import AttributeBrowser from '../../businessAttribute/AttributeBrowser';
import { useListBusinessAttributesQuery } from '../../../graphql/businessAttribute.generated';

export enum OperationType {
    ADD,
    REMOVE,
}

const AttributeSelect = styled(Select)`
    width: 480px;
`;

const AttributeName = styled.span`
    margin-left: 5px;
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
    white-space: normal;
    word-break: break-all;
`;

export const BrowserWrapper = styled.div<{ isHidden: boolean; width?: string; maxHeight?: number }>`
    background-color: white;
    border-radius: 5px;
    box-shadow: 0 3px 6px -4px rgb(0 0 0 / 12%), 0 6px 16px 0 rgb(0 0 0 / 8%), 0 9px 28px 8px rgb(0 0 0 / 5%);
    max-height: ${(props) => (props.maxHeight ? props.maxHeight : '380')}px;
    overflow: auto;
    position: absolute;
    transition: opacity 0.2s;
    width: ${(props) => (props.width ? props.width : '480px')};
    z-index: 1051;
    ${(props) =>
        props.isHidden &&
        `
        opacity: 0;
        height: 0;
    `}
`;

type EditAttributeModalProps = {
    open: boolean;
    onCloseModal: () => void;
    resources: ResourceRefInput[];
    type?: EntityType;
    operationType?: OperationType;
    onOkOverride?: (result: string) => void;
};

export default function EditBusinessAttributeModal({
    open,
    type = EntityType.BusinessAttribute,
    operationType = OperationType.ADD,
    onCloseModal,
    onOkOverride,
    resources,
}: EditAttributeModalProps) {
    const entityRegistry = useEntityRegistry();
    const [inputValue, setInputValue] = useState('');
    const [addBusinessAttributeMutation] = useAddBusinessAttributeMutation();
    const [removeBusinessAttributeMutation] = useRemoveBusinessAttributeMutation();
    const [isFocusedOnInput, setIsFocusedOnInput] = useState(false);
    const inputEl = useRef(null);
    const [urn, setUrn] = useState<string>('');
    const [disableAction, setDisableAction] = useState(false);
    const { recommendedData } = useGetRecommendations([EntityType.BusinessAttribute]);
    const [selectedAttribute, setSelectedAttribute] = useState<any>('');
    const [attributeSearch, { data: attributeSearchData }] = useGetSearchResultsLazyQuery();
    const attributeSearchResults =
        attributeSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];
    const { data: attributeData } = useListBusinessAttributesQuery({
        variables: {
            start: 0,
            count: 10,
            query: '',
        },
    });

    const displayedAttributes =
        attributeData?.listBusinessAttributes?.businessAttributes?.map((defaultValue) => ({
            urn: defaultValue.urn,
            component: (
                <div>
                    <GlobalOutlined />
                    <AttributeName>{defaultValue?.properties?.name}</AttributeName>
                </div>
            ),
        })) || [];

    const handleSearch = (text: string) => {
        if (text.length > 0) {
            attributeSearch({
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
        const displayName = entityRegistry.getDisplayName(entity.type, entity);
        return (
            <Select.Option
                data-testid="business-attribute-option"
                value={entity.urn}
                key={entity.urn}
                name={displayName}
            >
                <div>
                    <GlobalOutlined />
                    <AttributeName>{displayName}</AttributeName>
                </div>
            </Select.Option>
        );
    };

    const attributeResult = !inputValue || inputValue.length === 0 ? recommendedData : attributeSearchResults;
    const attributeSearchOptions = attributeResult?.map((result) => {
        return renderSearchResult(result);
    });

    const attributeRender = (props) => {
        // eslint-disable-next-line react/prop-types
        const { closable, onClose, value } = props;
        const onPreventMouseDown = (event) => {
            event.preventDefault();
            event.stopPropagation();
        };
        /* eslint-disable-next-line react/prop-types */
        const selectedItem = selectedAttribute
            ? selectedAttribute?.props?.component
            : displayedAttributes.find((attribute) => attribute.urn === value)?.component;
        return (
            <StyleTag onMouseDown={onPreventMouseDown} closable={closable} onClose={onClose}>
                {selectedItem}
            </StyleTag>
        );
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#addAttributeButton',
    });

    // When business attribute search result is selected, add the urn
    const onSelectValue = (selectedUrn: string) => {
        const selectedSearchOption = attributeSearchOptions?.find((option) => option.props.value === selectedUrn);
        setUrn(selectedUrn);
        if (selectedAttribute?.selectedUrn !== selectedUrn) {
            setSelectedAttribute({
                selectedUrn,
                component: (
                    <div>
                        <GlobalOutlined />
                        <AttributeName>{selectedSearchOption?.props?.name}</AttributeName>
                    </div>
                ),
            });
        }
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
    };

    // When a Tag or term search result is deselected, remove the urn from the Owners
    const onDeselectValue = (selectedUrn: string) => {
        setUrn(urn === selectedUrn ? '' : urn);
        setInputValue('');
        setIsFocusedOnInput(true);
        setSelectedAttribute('');
    };

    const addBusinessAttribute = () => {
        addBusinessAttributeMutation({
            variables: {
                input: {
                    businessAttributeUrn: urn,
                    resourceUrn: resources,
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content: `Added Business Attribute!`,
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
                setUrn('');
            });
    };

    const removeBusinessAttribute = () => {
        removeBusinessAttributeMutation({
            variables: {
                input: {
                    businessAttributeUrn: urn,
                    resourceUrn: [
                        {
                            resourceUrn: resources[0].resourceUrn,
                            subResource: resources[0].subResource,
                            subResourceType: resources[0].subResourceType,
                        },
                    ],
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content: `Removed Business Attribute!`,
                        duration: 2,
                    });
                }
            })
            .catch((e) => {
                message.destroy();
                message.error(
                    handleBatchError(urn, e, { content: `Failed to remove: \n ${e.message || ''}`, duration: 3 }),
                );
            })
            .finally(() => {
                setDisableAction(false);
                onCloseModal();
                setUrn('');
            });
    };

    const editBusinessAttribute = () => {
        if (operationType === OperationType.ADD) {
            addBusinessAttribute();
        } else {
            removeBusinessAttribute();
        }
    };

    const onOk = () => {
        if (onOkOverride) {
            onOkOverride(urn);
            return;
        }

        if (!resources) {
            onCloseModal();
            return;
        }
        setDisableAction(true);
        editBusinessAttribute();
    };

    function selectAttributeFromBrowser(selectedUrn: string, displayName: string) {
        setIsFocusedOnInput(false);
        setUrn(selectedUrn);
        setSelectedAttribute({
            selectedUrn,
            component: (
                <div>
                    <GlobalOutlined />
                    <AttributeName>{displayName}</AttributeName>
                </div>
            ),
        });
    }

    function clearInput() {
        setInputValue('');
        setTimeout(() => setIsFocusedOnInput(true), 0); // call after click outside
    }

    function handleBlur() {
        setInputValue('');
    }

    function handleKeyDown(event) {
        if (event.keyCode === ENTER_KEY_CODE) {
            (inputEl.current as any).blur();
        }
    }

    const isShowingAttributeBrowser = !inputValue && type === EntityType.BusinessAttribute && isFocusedOnInput;

    return (
        <Modal
            title={`${operationType === OperationType.ADD ? 'Add' : 'Remove'} ${entityRegistry.getEntityName(type)}s`}
            open={open}
            onCancel={onCloseModal}
            footer={
                <>
                    <Button onClick={onCloseModal} type="text">
                        Cancel
                    </Button>
                    <Button
                        id="addAttributeButton"
                        data-testid="add-attribute-from-modal-btn"
                        onClick={onOk}
                        disabled={urn.length === 0 || disableAction}
                    >
                        Done
                    </Button>
                </>
            }
        >
            <ClickOutside onClickOutside={() => setIsFocusedOnInput(false)}>
                <AttributeSelect
                    data-testid="business-attribute-modal-input"
                    placeholder={`Search for ${entityRegistry.getEntityName(type)?.toLowerCase()}...`}
                    autoFocus
                    defaultOpen
                    mode="multiple"
                    ref={inputEl}
                    filterOption={false}
                    showSearch
                    defaultActiveFirstOption={false}
                    onSelect={(asset: any) => {
                        onSelectValue(asset);
                    }}
                    onDeselect={(asset: any) => onDeselectValue(asset)}
                    onSearch={(value: string) => {
                        // eslint-disable-next-line react/prop-types
                        handleSearch(value.trim());
                        // eslint-disable-next-line react/prop-types
                        setInputValue(value.trim());
                    }}
                    tagRender={attributeRender}
                    value={urn || undefined}
                    onClear={clearInput}
                    onFocus={() => setIsFocusedOnInput(true)}
                    onBlur={handleBlur}
                    onInputKeyDown={handleKeyDown}
                    dropdownStyle={isShowingAttributeBrowser ? { display: 'none', color: 'RED' } : {}}
                >
                    {attributeSearchOptions}
                </AttributeSelect>
                <BrowserWrapper isHidden={!isShowingAttributeBrowser}>
                    <AttributeBrowser
                        isSelecting
                        selectAttribute={selectAttributeFromBrowser}
                        attributeData={attributeData}
                    />
                </BrowserWrapper>
            </ClickOutside>
        </Modal>
    );
}
