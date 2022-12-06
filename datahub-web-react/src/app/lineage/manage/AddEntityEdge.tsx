import { SubnodeOutlined } from '@ant-design/icons';
import { Select } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { useGetSearchResultsForMultipleLazyQuery } from '../../../graphql/search.generated';
import { Entity, EntityType, SearchResult } from '../../../types.generated';
import { Direction } from '../types';
import LineageEntityView from './LineageEntityView';

const AddEdgeWrapper = styled.div`
    padding: 15px 20px;
    display: flex;
    align-items: center;
`;

const AddLabel = styled.span`
    font-size: 12px;
    font-weight: bold;
    display: flex;
    align-items: center;
`;

const AddIcon = styled(SubnodeOutlined)`
    margin-right: 5px;
    font-size: 16px;
`;

const StyledSelect = styled(Select)`
    margin-left: 10px;
    flex: 1;
`;

export function existsInEntitiesToAdd(result: SearchResult, entitiesAlreadyAdded: Entity[]) {
    return !!entitiesAlreadyAdded.find((entity) => entity.urn === result.entity.urn);
}

interface Props {
    lineageDirection: Direction;
    setEntitiesToAdd: React.Dispatch<React.SetStateAction<Entity[]>>;
    entitiesToAdd: Entity[];
}

export default function AddEntityEdge({ lineageDirection, setEntitiesToAdd, entitiesToAdd }: Props) {
    const [queryText, setQueryText] = useState<string | undefined>(undefined);
    const [search, { data: searchData }] = useGetSearchResultsForMultipleLazyQuery();

    function handleSearch(text: string) {
        setQueryText(text);
        search({
            variables: {
                input: {
                    types: [EntityType.Dataset], // TODO: add specific types depending on the type of entity we're on
                    query: text,
                    start: 0,
                    count: 20,
                },
            },
        });
    }

    function selectEntity(urn: string) {
        const selectedEntity = searchData?.searchAcrossEntities?.searchResults.find(
            (result) => result.entity.urn === urn,
        );
        if (selectedEntity) {
            setEntitiesToAdd((existingEntities) => [...existingEntities, selectedEntity.entity]);
        }
    }

    const renderSearchResult = (entity: Entity) => {
        return (
            <Select.Option value={entity.urn} key={entity.urn}>
                <LineageEntityView entity={entity} displaySearchResult />
            </Select.Option>
        );
    };

    const searchResults = searchData?.searchAcrossEntities?.searchResults
        .filter((result) => !existsInEntitiesToAdd(result, entitiesToAdd))
        .map((result) => renderSearchResult(result.entity));

    return (
        <AddEdgeWrapper>
            <AddLabel>
                <AddIcon />
                Add {lineageDirection}
            </AddLabel>
            <StyledSelect
                showSearch
                placeholder="Search for entities to add..."
                onSearch={handleSearch}
                value={queryText}
                onSelect={(urn: any) => selectEntity(urn)}
            >
                {searchResults}
            </StyledSelect>
        </AddEdgeWrapper>
    );
}
