import { useEffect, useMemo } from 'react';
import { getSourceUrnFromSchemaFieldUrn } from '@src/app/entityV2/schemaField/utils';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { EntityType, FacetFilterInput, QuerySource } from '../../../../../../../types.generated';
import { useAggregateAcrossEntitiesQuery } from '../../../../../../../graphql/search.generated';
import { getV1FieldPathFromSchemaFieldUrn } from '../../../../../../lineageV2/lineageUtils';
import { getAndFilters } from '../utils/filterQueries';

interface Props {
    selectedColumnsFilter: FacetFilterInput;
    selectedUsersFilter: FacetFilterInput;
    setSelectedColumnsFilter: (columnsFilter: FacetFilterInput) => void;
}

export default function useColumnsFilter({
    selectedColumnsFilter,
    selectedUsersFilter,
    setSelectedColumnsFilter,
}: Props) {
    const { entityData } = useEntityData();
    const entityUrn = entityData?.urn;
    const siblingUrn = entityData?.siblingsSearch?.searchResults?.[0]?.entity?.urn;

    const values = siblingUrn ? [entityUrn as string, siblingUrn] : [entityUrn as string];
    const entityFilter = { field: 'entities', values };
    const sourceFilter = { field: 'source', values: [QuerySource.System] };
    const andFilters = getAndFilters({ ...selectedColumnsFilter, values: [] }, selectedUsersFilter, [
        entityFilter,
        sourceFilter,
    ]);
    const { data } = useAggregateAcrossEntitiesQuery({
        variables: {
            input: {
                facets: ['entities'],
                query: '*',
                types: [EntityType.Query],
                orFilters: [{ and: andFilters }],
                searchFlags: {
                    maxAggValues: 100,
                },
            },
        },
        skip: !entityUrn,
    });

    const aggregations = data?.aggregateAcrossEntities?.facets?.find(
        (facet) => facet.field === 'entities',
    )?.aggregations;

    const columnAggregations = useMemo(
        () =>
            aggregations
                ?.filter((agg) => agg.entity?.type === EntityType.SchemaField)
                ?.filter((agg) => {
                    const schemaFieldSourceUrn = getSourceUrnFromSchemaFieldUrn(agg.entity?.urn || '');
                    return schemaFieldSourceUrn === entityUrn || schemaFieldSourceUrn === siblingUrn;
                })
                ?.map((agg) => ({
                    value: agg.entity?.urn || '',
                    displayName: agg.entity?.urn ? getV1FieldPathFromSchemaFieldUrn(agg.entity.urn) : '',
                    count: agg.count,
                }))
                ?.sort((aggA, aggB) => aggB.count - aggA.count) || [],
        [aggregations, entityUrn, siblingUrn],
    );

    useEffect(() => {
        // if we get a selected column that doesn't exist in our aggregations, remove it.
        // this can happen with sibling schema field urns passed in from query params
        if (selectedColumnsFilter && columnAggregations && columnAggregations.length) {
            const currentSelectedValues = selectedColumnsFilter.values;
            const existingValues = columnAggregations.map((agg) => agg.value);
            const existingSelectedValues = currentSelectedValues?.filter((v) => existingValues.includes(v));
            if (currentSelectedValues?.length !== existingSelectedValues?.length) {
                setSelectedColumnsFilter({ ...selectedColumnsFilter, values: existingSelectedValues });
            }
        }
    }, [selectedColumnsFilter, columnAggregations, setSelectedColumnsFilter]);

    const columnsFilter = { aggregations: columnAggregations, displayName: 'Columns', field: 'entities' };

    return columnsFilter;
}
