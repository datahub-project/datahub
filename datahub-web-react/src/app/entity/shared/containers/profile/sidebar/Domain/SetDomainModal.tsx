import React, { useRef, useState } from 'react';
import { Button, Form, message, Modal, Select, Tag } from 'antd';
import styled from 'styled-components';

import { useGetSearchResultsLazyQuery } from '../../../../../../../graphql/search.generated';
import { Entity, EntityType } from '../../../../../../../types.generated';
import { useBatchSetDomainMutation } from '../../../../../../../graphql/mutations.generated';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { useEnterKeyListener } from '../../../../../../shared/useEnterKeyListener';
import { useGetRecommendations } from '../../../../../../shared/recommendation';
import { DomainLabel } from '../../../../../../shared/DomainLabel';

type Props = {
    urns: string[];
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

export const SetDomainModal = ({ urns, onCloseModal, refetch }: Props) => {
    const entityRegistry = useEntityRegistry();
    const [inputValue, setInputValue] = useState('');
    const [selectedDomain, setSelectedDomain] = useState<SelectedDomain | undefined>(undefined);
    const [domainSearch, { data: domainSearchData }] = useGetSearchResultsLazyQuery();
    const domainSearchResults =
        domainSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];
    const [batchSetDomainMutation] = useBatchSetDomainMutation();
    const [recommendedData] = useGetRecommendations([EntityType.Domain]);
    const inputEl = useRef(null);

    const onModalClose = () => {
        setInputValue('');
        setSelectedDomain(undefined);
        onCloseModal();
    };

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
    const renderSearchResult = (entity: Entity) => {
        const displayName = entityRegistry.getDisplayName(entity.type, entity);
        return (
            <Select.Option value={entity.urn} key={entity.urn}>
                <DomainLabel name={displayName} />
            </Select.Option>
        );
    };

    const domainResult = !inputValue || inputValue.length === 0 ? recommendedData : domainSearchResults;

    const domainSearchOptions = domainResult?.map((result) => {
        return renderSearchResult(result);
    });

    const onSelectDomain = (newUrn: string) => {
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
        const filteredDomains = domainResult?.filter((entity) => entity.urn === newUrn).map((entity) => entity) || [];
        if (filteredDomains.length) {
            const domain = filteredDomains[0];
            setSelectedDomain({
                displayName: entityRegistry.getDisplayName(EntityType.Domain, domain),
                type: EntityType.Domain,
                urn: newUrn,
            });
        }
    };

    const onDeselectDomain = () => {
        setInputValue('');
        setSelectedDomain(undefined);
    };

    const onOk = async () => {
        if (!selectedDomain) {
            return;
        }
        batchSetDomainMutation({
            variables: {
                input: {
                    resources: [...urns.map((urn) => ({ resourceUrn: urn }))],
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({ content: 'Updated Domain!', duration: 2 });
                    refetch?.();
                    onModalClose();
                    setSelectedDomain(undefined);
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to add assets to Domain: \n ${e.message || ''}`, duration: 3 });
            });
    };

    const selectValue = (selectedDomain && [selectedDomain?.displayName]) || undefined;

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#setDomainButton',
    });

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

    function handleBlur() {
        setInputValue('');
    }

    return (
        <Modal
            title="Set Domain"
            visible
            onCancel={onModalClose}
            footer={
                <>
                    <Button onClick={onModalClose} type="text">
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
                        filterOption={false}
                        showSearch
                        mode="multiple"
                        defaultActiveFirstOption={false}
                        placeholder="Search for Domains..."
                        onSelect={(domainUrn: any) => onSelectDomain(domainUrn)}
                        onDeselect={onDeselectDomain}
                        onSearch={(value: string) => {
                            // eslint-disable-next-line react/prop-types
                            handleSearch(value.trim());
                            // eslint-disable-next-line react/prop-types
                            setInputValue(value.trim());
                        }}
                        ref={inputEl}
                        value={selectValue}
                        tagRender={tagRender}
                        onBlur={handleBlur}
                    >
                        {domainSearchOptions}
                    </Select>
                </Form.Item>
            </Form>
        </Modal>
    );
};
