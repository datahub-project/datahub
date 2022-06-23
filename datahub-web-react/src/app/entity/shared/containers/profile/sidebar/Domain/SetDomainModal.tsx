import React, { useState } from 'react';
import { Button, Form, message, Modal, Select } from 'antd';

import { useGetSearchResultsLazyQuery } from '../../../../../../../graphql/search.generated';
import { Entity, EntityType } from '../../../../../../../types.generated';
import { useSetDomainMutation } from '../../../../../../../graphql/mutations.generated';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { useEntityData } from '../../../../EntityContext';
import { useEnterKeyListener } from '../../../../../../shared/useEnterKeyListener';
import { useGetRecommendations } from '../../../../../../shared/recommendation';
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

export const SetDomainModal = ({ visible, onCloseModal, refetch }: Props) => {
    const entityRegistry = useEntityRegistry();
    const { urn } = useEntityData();
    const [inputValue, setInputValue] = useState('');
    const [selectedDomain, setSelectedDomain] = useState<SelectedDomain | undefined>(undefined);
    const [domainSearch, { data: domainSearchData }] = useGetSearchResultsLazyQuery();
    const domainSearchResults =
        domainSearchData?.search?.searchResults?.map((searchResult) => searchResult.entity) || [];
    const [setDomainMutation] = useSetDomainMutation();
    const [recommendedData] = useGetRecommendations([EntityType.Domain]);
    const [form] = Form.useForm();

    const onModalClose = () => {
        setInputValue('');
        setSelectedDomain(undefined);
        form.resetFields();
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
        onModalClose();
    };

    const selectValue = (selectedDomain && [selectedDomain?.displayName]) || undefined;

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#setDomainButton',
    });

    return (
        <Modal
            title="Set Domain"
            visible={visible}
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
            <Form component={false} form={form}>
                <Form.Item>
                    <Select
                        autoFocus
                        defaultOpen
                        placeholder="Search for Domains..."
                        onSelect={(domainUrn: any) => onSelectDomain(domainUrn)}
                        onSearch={(value: string) => {
                            // eslint-disable-next-line react/prop-types
                            handleSearch(value.trim());
                            // eslint-disable-next-line react/prop-types
                            setInputValue(value.trim());
                        }}
                        value={selectValue}
                    >
                        {domainSearchOptions}
                    </Select>
                </Form.Item>
            </Form>
        </Modal>
    );
};
