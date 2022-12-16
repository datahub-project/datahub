import { SubnodeOutlined } from '@ant-design/icons';
import { Select } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useGetSearchResultsForMultipleLazyQuery } from '../../../graphql/search.generated';
import { Entity, EntityType, SearchResult } from '../../../types.generated';
import { Direction } from '../types';
import { getValidEntityTypes } from '../utils/manageLineageUtils';
import LineageEntityView from './LineageEntityView';
import EntityRegistry from '../../entity/EntityRegistry';

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

function getPlaceholderText(validEntityTypes: EntityType[], entityRegistry: EntityRegistry) {
    let placeholderText = 'Search for ';
    if (!validEntityTypes.length) {
        placeholderText = `${placeholderText} entities to add...`;
    } else if (validEntityTypes.length === 1) {
        placeholderText = `${placeholderText} ${entityRegistry.getCollectionName(validEntityTypes[0])}...`;
    } else {
        validEntityTypes.forEach((type, index) => {
            placeholderText = `${placeholderText} ${entityRegistry.getCollectionName(type)}${
                index !== validEntityTypes.length - 1 ? ', ' : '...'
            }`;
        });
    }
    return placeholderText;
}

export function existsInEntitiesToAdd(result: SearchResult, entitiesAlreadyAdded: Entity[]) {
    return !!entitiesAlreadyAdded.find((entity) => entity.urn === result.entity.urn);
}

interface Props {
    lineageDirection: Direction;
    setEntitiesToAdd: React.Dispatch<React.SetStateAction<Entity[]>>;
    entitiesToAdd: Entity[];
    entityUrn: string;
    entityType?: EntityType;
}

export default function AddEntityEdge({
    lineageDirection,
    setEntitiesToAdd,
    entitiesToAdd,
    entityUrn,
    entityType,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const [queryText, setQueryText] = useState<string | undefined>(undefined);
    const [search, { data: searchData }] = useGetSearchResultsForMultipleLazyQuery();

    const validEntityTypes = getValidEntityTypes(lineageDirection, entityType);

    function handleSearch(text: string) {
        setQueryText(text);
        search({
            variables: {
                input: {
                    types: validEntityTypes,
                    query: text,
                    start: 0,
                    count: 25,
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
        .filter((result) => !existsInEntitiesToAdd(result, entitiesToAdd) && result.entity.urn !== entityUrn)
        .map((result) => renderSearchResult(result.entity));

    const placeholderText = getPlaceholderText(validEntityTypes, entityRegistry);

    return (
        <AddEdgeWrapper>
            <AddLabel>
                <AddIcon />
                Add {lineageDirection}
            </AddLabel>
            <StyledSelect
                showSearch
                placeholder={placeholderText}
                onSearch={handleSearch}
                value={queryText}
                onSelect={(urn: any) => selectEntity(urn)}
            >
                {searchResults}
            </StyledSelect>
        </AddEdgeWrapper>
    );
}
