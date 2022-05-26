import React, { useState } from 'react';
// import { Select } from 'antd';
import Select from 'antd/lib/select';
import styled from 'styled-components';
import { Form } from 'antd';
import { gql, useQuery } from '@apollo/client';
import { useGetSearchResultsLazyQuery } from '../../../../../../graphql/search.generated';
import { useEntityRegistry } from '../../../../../useEntityRegistry';
import { EntityType, SearchResult } from '../../../../../../types.generated';

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
    const [containerSearch, { data: containerSearchData }] = useGetSearchResultsLazyQuery();
    const searchResults = containerSearchData?.search?.searchResults || [];
    const getParentContainer = gql`
        query search($urn: String!) {
            container(urn: $urn) {
                container {
                    properties {
                        name
                    }
                }
            }
        }
    `;
    const { data } = useQuery(getParentContainer, {
        variables: {
            urn: selectedContainers,
        },
        skip: selectedContainers === '',
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
        console.group(`we've picked ${urn}`);
    };
    const removeOption = () => {
        console.log(`removing pick`);
        setSelectedContainers('');
    };
    console.log(`parent container of selected is ${data?.container?.properties?.name}`);
    return (
        <>
            <Form.Item name="parentContainer" label="Specify a Container for the Dataset (Optional)">
                <Select
                    showSearch
                    autoFocus
                    filterOption={false}
                    value={entityRegistry.getDisplayName(EntityType.Container, selectedContainers)}
                    mode="multiple"
                    placeholder="Search for a parent container.."
                    onSearch={handleContainerSearch}
                    onSelect={(container: any) => onSelectMember(container)}
                    allowClear
                    onClear={removeOption}
                    onDeselect={removeOption}
                >
                    {searchResults?.map((result) => (
                        <Select.Option value={result.entity.urn}>{renderSearchResult(result)}</Select.Option>
                    ))}
                </Select>
            </Form.Item>
        </>
    );
};
