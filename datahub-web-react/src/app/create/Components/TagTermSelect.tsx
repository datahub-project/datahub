import { Select } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import TagLabel from '../../shared/TagLabel';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useGetSearchResultsLazyQuery } from '../../../graphql/search.generated';
import { EntityType, SearchResult, Tag } from '../../../types.generated';

export const TagTermSelect = () => {
    const TagSelect = styled(Select)`
        padding-left: 1px;
        padding-right: 1px;
    `;
    const entityRegistry = useEntityRegistry();
    const [inputValue, setInputValue] = useState('');
    const [tagTermSearch, { data: tagTermSearchData }] = useGetSearchResultsLazyQuery();
    const tagSearchResults = tagTermSearchData?.search?.searchResults || [];
    const renderSearchResult = (result: SearchResult) => {
        const displayName =
            result.entity.type === EntityType.Tag
                ? (result.entity as Tag).name
                : entityRegistry.getDisplayName(result.entity.type, result.entity);
        const tagOrTermComponent =
            result.entity.type === EntityType.Tag ? (
                <TagLabel
                    name={displayName}
                    colorHash={(result.entity as Tag).urn}
                    color={(result.entity as Tag).properties?.colorHex}
                />
            ) : (
                <TagLabel
                    name={displayName}
                    colorHash={(result.entity as Tag).urn}
                    color={(result.entity as Tag).properties?.colorHex}
                />
            );
        return (
            <Select.Option value={result.entity.urn} key={result.entity.urn} name={displayName}>
                {tagOrTermComponent}
            </Select.Option>
        );
    };
    const tagSearchOptions = tagSearchResults.map((result) => {
        return renderSearchResult(result);
    });
    console.log(`inputvalue is ${inputValue}`);
    const handleSearch = (text: string) => {
        console.log(`value to search is ${text}`);
        if (text.length > 0) {
            tagTermSearch({
                variables: {
                    input: {
                        type: EntityType.Tag,
                        query: text,
                        start: 0,
                        count: 10,
                    },
                },
            });
        }
    };
    return (
        <>
            <TagSelect
                autoFocus
                mode="multiple"
                filterOption={false}
                placeholder="Search for existing tags..."
                showSearch
                onSearch={(value: string) => {
                    // eslint-disable-next-line react/prop-types
                    handleSearch(value.trim());
                    // eslint-disable-next-line react/prop-types
                    setInputValue(value.trim());
                }}
            >
                {tagSearchOptions}
            </TagSelect>
        </>
    );
};
