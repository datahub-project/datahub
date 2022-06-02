import React, { useState } from 'react';
// import { Select } from 'antd';
import Select from 'antd/lib/select';
import styled from 'styled-components';
import { Form } from 'antd';
// import { gql, useQuery } from '@apollo/client';
import { useGetSearchResultsLazyQuery } from '../../../../../../graphql/search.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { EntityType, SearchResult } from '../../../../../../types.generated';
// import { useGetContainerLazyQuery } from '../../../../../../graphql/container.generated';
// import { useGetContainerLazyQuery } from '../../../../../../graphql/container.generated';

// import Link from 'antd/lib/typography/Link';

const SearchResultContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 2px;
`;

const SearchResultContent = styled.div`
    display: flex;
    justify-content: start;
    align-items: center;
`;

const SearchResultDisplayName = styled.div`
    margin-left: 5px;
`;

interface Props {
    platformType: string;
}

export const SetParentContainer = (props: Props) => {
    // need this to render the display name of the container
    // decided not to put name of parent container of selected container - the new feature in 0.8.36 would be better
    const entityRegistry = useEntityRegistry();
    const [selectedContainers, setSelectedContainers] = useState('');
    const [containerSearch, { data: containerSearchData }] = useGetSearchResultsLazyQuery();
    const searchResults = containerSearchData?.search?.searchResults || [];
    const renderSearchResult = (result: SearchResult) => {
        const displayName = entityRegistry.getDisplayName(result.entity.type, result.entity);
        console.log(`display name is ${displayName}`);
        return (
            <SearchResultContainer>
                <SearchResultContent>
                    <SearchResultDisplayName>
                        <div>{displayName}</div>
                    </SearchResultDisplayName>
                </SearchResultContent>
            </SearchResultContainer>
        );
    };
    const handleContainerSearch = (text: string) => {
        if (text.length > 0) {
            containerSearch({
                variables: {
                    input: {
                        type: EntityType.Container,
                        query: text,
                        start: 0,
                        count: 5,
                        filters: [
                            {
                                field: 'platform',
                                value: props.platformType,
                            },
                        ],
                    },
                },
            });
        }
    };
    const onSelectMember = (urn: string) => {
        setSelectedContainers(urn);
    };
    const removeOption = () => {
        console.log(`removing ${selectedContainers}`);
        setSelectedContainers('');
    };
    return (
        <>
            <Form.Item
                name="parentContainerSelect"
                label="Specify a Container for the Dataset (Optional)"
                rules={[
                    {
                        required: true,
                        message: 'A container must be specified.',
                    },
                ]}
            >
                <Select
                    style={{ width: 200 }}
                    showSearch
                    autoFocus
                    filterOption={false}
                    value={JSON.stringify(selectedContainers)}
                    mode="multiple"
                    showArrow={false}
                    placeholder="Search for a parent container.."
                    onSearch={handleContainerSearch}
                    onSelect={(container: any) => onSelectMember(container)}
                    allowClear
                    onClear={removeOption}
                    onDeselect={removeOption}
                >
                    {searchResults?.map((result) => (
                        <Select.Option disabled={selectedContainers !== ''} value={result.entity.urn}>
                            {renderSearchResult(result)}
                        </Select.Option>
                    ))}
                </Select>
            </Form.Item>
        </>
    );
};
