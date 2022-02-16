import { Button, Form, message, Modal, Select, Tag } from 'antd';
import React, { useRef, useState } from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { useGetSearchResultsLazyQuery } from '../../../../../../../graphql/search.generated';
import { EntityType, SearchResult } from '../../../../../../../types.generated';
import { useSetDomainMutation } from '../../../../../../../graphql/mutations.generated';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { useEntityData } from '../../../../EntityContext';
import { useEnterKeyListener } from '../../../../../../shared/useEnterKeyListener';

type Props = {
    visible: boolean;
    onClose: () => void;
    refetch?: () => Promise<any>;
};

const SearchResultContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px;
`;

const SearchResultContent = styled.div`
    display: flex;
    justify-content: start;
    align-items: center;
`;

const SearchResultDisplayName = styled.div`
    margin-left: 12px;
`;

type SelectedDomain = {
    displayName: string;
    type: EntityType;
    urn: string;
};

export const SetDomainModal = ({ visible, onClose, refetch }: Props) => {
    const entityRegistry = useEntityRegistry();
    const { urn } = useEntityData();
    const [selectedDomain, setSelectedDomain] = useState<SelectedDomain | undefined>(undefined);
    const [domainSearch, { data: domainSearchData }] = useGetSearchResultsLazyQuery();
    const domainSearchResults = domainSearchData?.search?.searchResults || [];
    const [setDomainMutation] = useSetDomainMutation();

    const inputEl = useRef(null);

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
        onClose();
    };

    const onSelectDomain = (newUrn: string) => {
        if (inputEl && inputEl.current) {
            (inputEl.current as any).blur();
        }
        const filteredDomains =
            domainSearchResults?.filter((result) => result.entity.urn === newUrn).map((result) => result.entity) || [];
        if (filteredDomains.length) {
            const domain = filteredDomains[0];
            setSelectedDomain({
                displayName: entityRegistry.getDisplayName(EntityType.Domain, domain),
                type: EntityType.Domain,
                urn: newUrn,
            });
        }
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

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#setDomainButton',
    });

    const renderSearchResult = (result: SearchResult) => {
        const displayName = entityRegistry.getDisplayName(result.entity.type, result.entity);
        return (
            <SearchResultContainer>
                <SearchResultContent>
                    <SearchResultDisplayName>
                        <div>{displayName}</div>
                    </SearchResultDisplayName>
                </SearchResultContent>
                <Link
                    target="_blank"
                    rel="noopener noreferrer"
                    to={() => `/${entityRegistry.getPathName(result.entity.type)}/${result.entity.urn}`}
                >
                    View
                </Link>{' '}
            </SearchResultContainer>
        );
    };

    const selectValue = (selectedDomain && [selectedDomain?.displayName]) || [];

    return (
        <Modal
            title="Set Domain"
            visible={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
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
                        value={selectValue}
                        mode="multiple"
                        ref={inputEl}
                        placeholder="Search for Domains..."
                        onSelect={(domainUrn: any) => onSelectDomain(domainUrn)}
                        onDeselect={() => setSelectedDomain(undefined)}
                        onSearch={handleSearch}
                        filterOption={false}
                        tagRender={(tagProps) => <Tag>{tagProps.value}</Tag>}
                    >
                        {domainSearchResults.map((result) => {
                            return (
                                <Select.Option value={result.entity.urn}>{renderSearchResult(result)}</Select.Option>
                            );
                        })}
                    </Select>
                </Form.Item>
            </Form>
        </Modal>
    );
};
