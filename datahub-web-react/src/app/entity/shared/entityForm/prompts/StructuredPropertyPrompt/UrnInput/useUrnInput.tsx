import { Tag } from 'antd';
import React, { useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import { isEqual } from 'lodash';
import { Entity, PropertyCardinality, StructuredPropertyEntity } from '../../../../../../../types.generated';
import { useGetSearchResultsForMultipleLazyQuery } from '../../../../../../../graphql/search.generated';
import usePrevious from '../../../../../../shared/usePrevious';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { useEntityData } from '../../../../EntityContext';
import { getInitialEntitiesForUrnPrompt } from '../utils';
import SelectedEntity from './SelectedEntity';

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
    structuredProperty: StructuredPropertyEntity;
    selectedValues: any[];
    updateSelectedValues: (values: any[]) => void;
}

export default function useUrnInput({ structuredProperty, selectedValues, updateSelectedValues }: Props) {
    const entityRegistry = useEntityRegistry();
    const { entityData } = useEntityData();
    const initialEntities = useMemo(
        () => getInitialEntitiesForUrnPrompt(structuredProperty.urn, entityData, selectedValues),
        [structuredProperty.urn, entityData, selectedValues],
    );

    // we store the selected entity objects here to render display name, platform, etc.
    // selectedValues contains a list of urns that we store for the structured property values
    const [selectedEntities, setSelectedEntities] = useState<Entity[]>(initialEntities);
    const [searchAcrossEntities, { data: searchData, loading }] = useGetSearchResultsForMultipleLazyQuery();
    const searchResults =
        searchData?.searchAcrossEntities?.searchResults?.map((searchResult) => searchResult.entity) || [];
    const allowedEntityTypes = structuredProperty.definition.typeQualifier?.allowedTypes?.map(
        (allowedType) => allowedType.info.type,
    );
    const entityTypeNames: string[] | undefined = allowedEntityTypes?.map(
        (entityType) => entityRegistry.getEntityName(entityType) || '',
    );
    const isMultiple = structuredProperty.definition.cardinality === PropertyCardinality.Multiple;

    const previousEntityUrn = usePrevious(entityData?.urn);
    const previousInitial = usePrevious(initialEntities);

    useEffect(() => {
        if (entityData?.urn !== previousEntityUrn || !isEqual(previousInitial, initialEntities)) {
            setSelectedEntities(initialEntities || []);
        }
    }, [entityData?.urn, previousEntityUrn, initialEntities, previousInitial]);

    function handleSearch(query: string) {
        if (query.length > 0) {
            searchAcrossEntities({ variables: { input: { query, types: allowedEntityTypes } } });
        }
    }

    const onSelectValue = (urn: string) => {
        const newValues = isMultiple ? [...selectedValues, urn] : [urn];
        updateSelectedValues(newValues);

        const selectedEntity = searchResults?.find((result) => result.urn === urn) as Entity;
        const newEntities = isMultiple ? [...selectedEntities, selectedEntity] : [selectedEntity];
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
        onDeselectValue,
        selectedEntities,
        searchResults,
        loading,
        entityTypeNames,
    };
}
