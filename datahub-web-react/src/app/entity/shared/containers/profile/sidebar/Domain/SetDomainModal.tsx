import React, { useEffect, useRef, useState } from 'react';
import { Button, Form, message, Modal, Select, Tag } from 'antd';
import styled from 'styled-components';

import { useGetSearchResultsLazyQuery } from '../../../../../../../graphql/search.generated';
import { EntityType, SearchResult } from '../../../../../../../types.generated';
import { useSetDomainMutation } from '../../../../../../../graphql/mutations.generated';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { useEntityData } from '../../../../EntityContext';
import { useEnterKeyListener } from '../../../../../../shared/useEnterKeyListener';
import { useGetRecommendedDomains } from '../../../../../../shared/recommendation';
import { DomainLabel } from '../../../../../../shared/DomainLabel';

type Props = {
    visible: boolean;
    onCloseModal: () => void;
    refetch?: () => Promise<any>;
};

type SelectedDomain = {
    displayName: string;
    type: EntityType;
    urn: string;
};

const StyleTag = styled(Tag)`
    padding: 0px 7px;
    margin-right: 3px;
    display: flex;
    justify-content: start;
    align-items: center;
`;

export const SetDomainModal = ({ visible, onCloseModal, refetch }: Props) => {
    const entityRegistry = useEntityRegistry();
    const [inputValue, setInputValue] = useState('');
    const { urn } = useEntityData();
    const [selectedDomain, setSelectedDomain] = useState<SelectedDomain | undefined>(undefined);
    const [domainSearch, { data: domainSearchData }] = useGetSearchResultsLazyQuery();
    const domainSearchResults = domainSearchData?.search?.searchResults || [];
    const [setDomainMutation] = useSetDomainMutation();

    const inputEl = useRef(null);

    useEffect(() => {
        setTimeout(() => {
            if (inputEl && inputEl.current) {
                (inputEl.current as any).focus();
            }
        }, 1);
    });

    const recommendedDomainsData = useGetRecommendedDomains();

    const handleSearch = (text: string) => {
        if (text.length > 2) {
            domainSearch({
                variables: {
                    input: {
                        type: EntityType.Domain,
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
        const displayName = entityRegistry.getDisplayName(result.entity.type, result.entity);
        return (
            <Select.Option value={result.entity.urn} key={result.entity.urn}>
                <DomainLabel name={displayName} />
            </Select.Option>
        );
    };

    const domainResult = !inputValue || inputValue.length === 0 ? recommendedDomainsData : domainSearchResults;

    const domainSearchOptions = domainResult?.map((result) => {
        return renderSearchResult(result);
    });

    const onSelectDomain = (newUrn: string) => {
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
        const filteredDomains =
            domainResult?.filter((result) => result.entity.urn === newUrn).map((result) => result.entity) || [];
        if (filteredDomains.length) {
            const domain = filteredDomains[0];
            setSelectedDomain({
                displayName: entityRegistry.getDisplayName(EntityType.Domain, domain),
                type: EntityType.Domain,
                urn: newUrn,
            });
        }
    };

    const onOk = async () => {
        if (!selectedDomain) {
            return;
        }
        try {
            await setDomainMutation({
                variables: {
                    entityUrn: urn,
                    domainUrn: selectedDomain.urn,
                },
            });
            message.success({ content: 'Updated Domain!', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to set Domain: \n ${e.message || ''}`, duration: 3 });
            }
        }
        setSelectedDomain(undefined);
        refetch?.();
        onCloseModal();
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

    const selectValue = (selectedDomain && [selectedDomain?.displayName]) || [];

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#setDomainButton',
    });

    function clearInput() {
        setInputValue('');
    }

    function handleBlur() {
        setInputValue('');
    }

    return (
        <Modal
            title="Set Domain"
            visible={visible}
            onCancel={onCloseModal}
            footer={
                <>
                    <Button onClick={onCloseModal} type="text">
                        Cancel
                    </Button>
                    <Button id="setDomainButton" disabled={selectedDomain === undefined} onClick={onOk}>
                        Add
                    </Button>
                </>
            }
        >
            <Form component={false}>
                <Form.Item>
                    <Select
                        autoFocus
                        defaultOpen
                        mode="multiple"
                        ref={inputEl}
                        placeholder="Search for Domains..."
                        showSearch
                        filterOption={false}
                        defaultActiveFirstOption={false}
                        onSelect={(domainUrn: any) => onSelectDomain(domainUrn)}
                        onDeselect={() => setSelectedDomain(undefined)}
                        onSearch={(value: string) => {
                            // eslint-disable-next-line react/prop-types
                            handleSearch(value.trim());
                            // eslint-disable-next-line react/prop-types
                            setInputValue(value.trim());
                        }}
                        tagRender={tagRender}
                        value={selectValue}
                        onClear={clearInput}
                        onBlur={handleBlur}
                    >
                        {domainSearchOptions}
                    </Select>
                </Form.Item>
            </Form>
        </Modal>
    );
};
