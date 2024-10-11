import { Tag } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { Entity, EntityType } from '../../../../../../../types.generated';
import {
    useGetAutoCompleteMultipleResultsLazyQuery,
    useGetSearchResultsForMultipleQuery,
} from '../../../../../../../graphql/search.generated';
import { useEntityData } from '../../../../EntityContext';
import SelectedEntity from './SelectedEntity';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import usePrevious from '../../../../../../shared/usePrevious';

const StyleTag = styled(Tag)`
    margin: 2px;
    padding: 4px 6px;
    display: flex;
    justify-content: start;
    align-items: center;
    white-space: nowrap;
    opacity: 1;
    color: #434343;
    line-height: 16px;
    font-size: 12px;
    max-width: 100%;
`;

interface Props {
    selectedValues: any[];
    updateSelectedValues: (values: any[]) => void;
    initialEntities: Entity[];
    allowedEntities?: Entity[];
    allowedEntityTypes?: EntityType[];
    isMultiple: boolean;
}

export default function useUrnInput({
    initialEntities,
    allowedEntities,
    selectedValues,
    updateSelectedValues,
    allowedEntityTypes,
    isMultiple,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const { entityData } = useEntityData();

    // we store the selected entity objects here to render display name, platform, etc.
    // selectedValues contains a list of urns that we store for the structured property values
    const [isFocused, setIsFocused] = useState(false);
    const [searchValue, setSearchValue] = useState<string>('');
    const [selectedEntities, setSelectedEntities] = useState<Entity[]>(initialEntities);
    const { data: initialData } = useGetSearchResultsForMultipleQuery({
        variables: { input: { query: '*', types: allowedEntityTypes, count: 5 } },
        skip: !!allowedEntities?.length,
    });
    const [getAutoCompleteResults, { data: autoCompleteData, loading }] = useGetAutoCompleteMultipleResultsLazyQuery();
    let entityOptions: Entity[] =
        autoCompleteData?.autoCompleteForMultiple?.suggestions?.flatMap((result) => result.entities) ||
        allowedEntities?.filter((e) =>
            entityRegistry.getDisplayName(e.type, e).toLocaleLowerCase().includes(searchValue.toLocaleLowerCase()),
        ) ||
        [];
    if (!entityOptions.length && !searchValue) {
        entityOptions = initialData?.searchAcrossEntities?.searchResults.map((r) => r.entity) || [];
    }
    const entityTypeNames: string[] | undefined = allowedEntityTypes?.map(
        (entityType) => entityRegistry.getCollectionName(entityType) || '',
    );

    const previousEntityUrn = usePrevious(entityData?.urn);
    useEffect(() => {
        if (entityData?.urn !== previousEntityUrn) {
            setSelectedEntities(initialEntities || []);
        }
    }, [entityData?.urn, previousEntityUrn, initialEntities]);

    function handleSearch(query: string) {
        setSearchValue(query);
        if (!allowedEntities?.length && query.length > 0) {
            getAutoCompleteResults({ variables: { input: { query, types: allowedEntityTypes } } });
        }
    }

    const onSelectValue = (urn: string) => {
        const newValues = isMultiple ? [...selectedValues, urn] : [urn];
        updateSelectedValues(newValues);

        const selectedEntity = entityOptions?.find((result) => result.urn === urn) as Entity;

        const newEntities = isMultiple ? [...selectedEntities, selectedEntity] : [selectedEntity];
        setSelectedEntities(newEntities);
    };

    const onSelectEntity = (entity: Entity) => {
        const newValues = isMultiple ? [...selectedValues, entity.urn] : [entity.urn];
        updateSelectedValues(newValues);

        const newEntities = isMultiple ? [...selectedEntities, entity] : [entity];
        setSelectedEntities(newEntities);
    };

    const onDeselectValue = (urn: string) => {
        const newValues = selectedValues.filter((value) => value !== urn);
        updateSelectedValues(newValues);

        const newSelectedEntities = selectedEntities.filter((entity) => entity.urn !== urn);
        setSelectedEntities(newSelectedEntities);
    };

    const tagRender = (props: any) => {
        // eslint-disable-next-line react/prop-types
        const { closable, onClose, value } = props;
        const selectedEntity = selectedEntities.find((term) => term.urn === value);

        if (!selectedEntity) return <></>;

        return (
            <StyleTag closable={closable} onClose={onClose}>
                <SelectedEntity entity={selectedEntity} />
            </StyleTag>
        );
    };

    return {
        tagRender,
        handleSearch,
        onSelectValue,
        onSelectEntity,
        onDeselectValue,
        selectedEntities,
        entityOptions,
        loading,
        entityTypeNames,
        searchValue,
        setSearchValue,
        isFocused,
        setIsFocused,
    };
}
