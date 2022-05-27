import React, { useEffect, useState } from 'react';
// import { Select } from 'antd';
import Select from 'antd/lib/select';
import styled from 'styled-components';
import { Form } from 'antd';
// import { gql, useQuery } from '@apollo/client';
import { useGetSearchResultsLazyQuery } from '../../../../../../graphql/search.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { EntityType, SearchResult } from '../../../../../../types.generated';
import { useGetContainerLazyQuery } from '../../../../../../graphql/container.generated';
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
    const entityRegistry = useEntityRegistry();
    const [selectedContainers, setSelectedContainers] = useState('');
    const [parentContainer, setParentContainer] = useState('');
    const [containerSearch, { data: containerSearchData }] = useGetSearchResultsLazyQuery();
    const [parentCon, { data: parentConData }] = useGetContainerLazyQuery({
        variables: {
            urn: selectedContainers,
        },
    });
    const searchResults = containerSearchData?.search?.searchResults || [];

    useEffect(() => {
        if (selectedContainers !== '') {
            parentCon({
                variables: {
                    urn: selectedContainers,
                },
            });
        }
    }, [parentCon, selectedContainers]);
    useEffect(() => {
        setParentContainer(parentConData?.container?.container?.urn || '');
    }, [parentConData]);
    console.log(`the current parentcontainer is ${parentContainer}`);
    // this portion is copied from AddGroupMembersModal - it nicely renders the result list
    const renderSearchResult = (result: SearchResult) => {
        const displayName = entityRegistry.getDisplayName(result.entity.type, result.entity);
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
        console.group(`Selected ${urn} as the container for dataset`);
    };
    const removeOption = () => {
        console.log(`removing ${selectedContainers}`);
        setSelectedContainers('');
        setParentContainer('');
    };
    return (
        <>
            <Form.Item name="parentContainerSelect" label="Specify a Container for the Dataset (Optional)">
                <Select
                    style={{ width: 200 }}
                    showSearch
                    autoFocus
                    filterOption={false}
                    value={entityRegistry.getDisplayName(EntityType.Container, selectedContainers)}
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
