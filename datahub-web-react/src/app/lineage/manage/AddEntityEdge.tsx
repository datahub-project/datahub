import { LoadingOutlined, SubnodeOutlined } from '@ant-design/icons';
import { AutoComplete, Empty } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '../../../graphql/search.generated';
import { Entity, EntityType } from '../../../types.generated';
import { Direction } from '../types';
import { getValidEntityTypes } from '../utils/manageLineageUtils';
import LineageEntityView from './LineageEntityView';
import EntityRegistry from '../../entity/EntityRegistry';
import { ANTD_GRAY } from '../../entity/shared/constants';

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

const StyledAutoComplete = styled(AutoComplete)`
    margin-left: 10px;
    flex: 1;
`;

const LoadingWrapper = styled.div`
    padding: 8px;
    display: flex;
    justify-content: center;

    svg {
        height: 15px;
        width: 15px;
        color: ${ANTD_GRAY[8]};
    }
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

export function existsInEntitiesToAdd(result: Entity, entitiesAlreadyAdded: Entity[]) {
    return !!entitiesAlreadyAdded.find((entity) => entity.urn === result.urn);
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
    const [getAutoCompleteResults, { data: autoCompleteResults, loading }] =
        useGetAutoCompleteMultipleResultsLazyQuery();
    const [queryText, setQueryText] = useState<string>('');

    const validEntityTypes = getValidEntityTypes(lineageDirection, entityType);

    function handleSearch(text: string) {
        setQueryText(text);
        if (text !== '') {
            getAutoCompleteResults({
                variables: {
                    input: {
                        types: validEntityTypes,
                        query: text,
                        limit: 15,
                    },
                },
            });
        }
    }

    function selectEntity(urn: string) {
        const resultEntities = autoCompleteResults?.autoCompleteForMultiple?.suggestions?.flatMap(
            (suggestion) => suggestion.entities || [],
        );
        const selectedEntity = resultEntities?.find((entity) => entity.urn === urn);
        if (selectedEntity) {
            setEntitiesToAdd((existingEntities) => [...existingEntities, selectedEntity]);
        }
    }

    const renderSearchResult = (entity: Entity) => {
        return (
            <AutoComplete.Option value={entity.urn} key={entity.urn}>
                <LineageEntityView entity={entity} displaySearchResult />
            </AutoComplete.Option>
        );
    };

    const searchResults = autoCompleteResults?.autoCompleteForMultiple?.suggestions
        .flatMap((suggestion) => suggestion.entities || [])
        .filter((entity) => entity && !existsInEntitiesToAdd(entity, entitiesToAdd) && entity.urn !== entityUrn)
        .map((entity) => renderSearchResult(entity));

    const placeholderText = getPlaceholderText(validEntityTypes, entityRegistry);

    return (
        <AddEdgeWrapper>
            <AddLabel>
                <AddIcon />
                Add {lineageDirection}
            </AddLabel>
            <StyledAutoComplete
                autoFocus
                showSearch
                value={queryText}
                placeholder={placeholderText}
                onSearch={handleSearch}
                onSelect={(urn: any) => selectEntity(urn)}
                filterOption={false}
                notFoundContent={(queryText.length > 3 && <Empty description="No Assets Found" />) || undefined}
            >
                {loading && (
                    <AutoComplete.Option value="loading">
                        <LoadingWrapper>
                            <LoadingOutlined />
                        </LoadingWrapper>
                    </AutoComplete.Option>
                )}
                {searchResults}
            </StyledAutoComplete>
        </AddEdgeWrapper>
    );
}
