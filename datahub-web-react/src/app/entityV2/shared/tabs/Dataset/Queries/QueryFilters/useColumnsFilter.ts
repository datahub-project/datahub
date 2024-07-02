import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { EntityType, FacetFilterInput, QuerySource } from '../../../../../../../types.generated';
import { useAggregateAcrossEntitiesQuery } from '../../../../../../../graphql/search.generated';
import {
    getSourceUrnFromSchemaFieldUrn,
    getV1FieldPathFromSchemaFieldUrn,
} from '../../../../../../lineageV2/lineageUtils';
import { getAndFilters } from '../utils/filterQueries';

interface Props {
    selectedColumnsFilter: FacetFilterInput;
    selectedUsersFilter: FacetFilterInput;
}

export default function useColumnsFilter({ selectedColumnsFilter, selectedUsersFilter }: Props) {
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

    const columnAggregations =
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
            ?.sort((aggA, aggB) => aggB.count - aggA.count) || [];

    const columnsFilter = { aggregations: columnAggregations, displayName: 'Columns', field: 'entities' };

    return columnsFilter;
}
